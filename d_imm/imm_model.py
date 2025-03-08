import time
import numpy as np
import pandas as pd
from collections import defaultdict, namedtuple

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col, lit, udf, expr, sqrt, sum as Fsum
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import IntegerType, DoubleType, ArrayType, StructType, StructField

from pyspark.ml.clustering import KMeans as SparkKMeans
from pyspark.ml.linalg import DenseVector
from pyspark.ml.functions import vector_to_array

try:
    from graphviz import Source
    graphviz_available = True
except Exception:
    graphviz_available = False


# -- Utility Functions --
def serialize_tree(node):
    """Convert the tree object into a JSON-serializable dictionary."""
    if node is None:
        return None
    return {
        "feature": node.feature,
        "value": node.value,
        "samples": node.samples,
        "mistakes": node.mistakes,
        "left": serialize_tree(node.left),
        "right": serialize_tree(node.right),
    }


def _predict_subtree(node, features):
    """Recursively predict clusters based on the serialized tree."""
    if node is None or "feature" not in node:
        return -1  # Default cluster if something is wrong

    if node["left"] is None and node["right"] is None:
        return node["value"]

    if features[node["feature"]] <= node["value"]:
        return _predict_subtree(node["left"], features)
    else:
        return _predict_subtree(node["right"], features)


# -- Node Class --
class Node:
    def __init__(self):
        self.feature = None
        self.value = None
        self.samples = None
        self.mistakes = None
        self.left = None
        self.right = None

    def is_leaf(self):
        return (self.left is None) and (self.right is None)

    def set_condition(self, feature, value):
        self.feature = feature
        self.value = value


Split = namedtuple("Split", ["feature_index", "threshold", "categories", "is_continuous"])


class DistributedIMM:
    def __init__(self, spark, k, max_leaves=None, verbose=0, n_jobs=None, example_count=10000, split_count=32):
        """
        Initialize the DistributedIMM class with Spark session and parameters.
        """
        self.spark = spark
        self.k = k
        self.tree = None
        self.max_leaves = max_leaves if max_leaves else k
        self.verbose = verbose
        self.n_jobs = n_jobs if n_jobs else 1
        self._feature_importance = None
        self.nodes = []
        self.split_count = split_count
        self.histogram_example_count = example_count
        self.histogram = None

    # -- Helper Methods --
    def _to_features_array(self, df: DataFrame) -> DataFrame:
        """Ensure DataFrame has a 'features_array' column."""
        if "features_array" not in df.columns:
            df = df.withColumn("features_array", vector_to_array(col("features")))
        return df

    def _log_time(self, start_time, end_time, msg, verbose_level=2):
        """Log elapsed time if verbosity is high enough."""
        if self.verbose > verbose_level:
            elapsed = end_time - start_time
            minutes, seconds = divmod(elapsed, 60)
            print(f"{msg}: {int(minutes)} minutes and {seconds:.2f} seconds")

    def _predict_dataframe(self, x_data: DataFrame) -> DataFrame:
        """Generate predictions and ensure features are in array format."""
        return self._to_features_array(self.predict(x_data))

    def _compute_cluster_means(self, predicted_data: DataFrame, feature_dim: int) -> DataFrame:
        """Compute cluster means from predicted data."""
        means_expr = [F.mean(col("features_array")[i]).alias(f"feature_{i}_mean") for i in range(feature_dim)]
        cluster_means = predicted_data.groupBy("prediction").agg(*means_expr)
        cluster_means = cluster_means.withColumn(
            "cluster_mean", F.array(*[col(f"feature_{i}_mean") for i in range(feature_dim)])
        )
        return cluster_means.select("prediction", "cluster_mean")

    def _join_exploded(self, df1: DataFrame, col1: str, df2: DataFrame, col2: str) -> DataFrame:
        """
        Explode two array columns and join them on prediction and position.
        Returns a DataFrame with squared differences between the two arrays.
        """
        exploded1 = df1.selectExpr("prediction", f"posexplode({col1}) as (pos, val1)")
        exploded2 = df2.selectExpr("prediction", f"posexplode({col2}) as (pos, val2)")
        joined = exploded1.alias("a").join(
            exploded2.alias("b"),
            (col("a.prediction") == col("b.prediction")) & (col("a.pos") == col("b.pos"))
        )
        return joined.selectExpr("a.prediction as prediction", "(a.val1 - b.val2)*(a.val1 - b.val2) as squared_distance")

    # -- Main Methods --
    def fit(self, x_data: DataFrame, kmeans_model=None):
        """
        Build a threshold tree from the training set x_data.
        """
        if self.verbose > 0:
            print("Running 'fit' method")

        # Cluster data using k-means if a model is not provided
        if kmeans_model is None:
            if self.verbose > 0:
                print(f"Training kmeans with {self.k} clusters")
            kmeans = SparkKMeans().setK(self.k).setSeed(1).setMaxIter(40).setFeaturesCol("features")
            kmeans_model = kmeans.fit(x_data)
        else:
            assert kmeans_model.getK() == self.k, "KMeans model must have the same number of clusters as 'k'"

        # Get predictions and cluster centers
        clustered_data = kmeans_model.transform(x_data).select("features", "prediction")
        self.all_centers = kmeans_model.clusterCenters()
        self.centers_broadcast = self.spark.sparkContext.broadcast(self.all_centers)

        if self.verbose > 3:
            print("Cluster centers:", self.all_centers)
            print("Sample of clustered data:")
            clustered_data.show(5)

        valid_centers = [True] * self.k
        feature_count = len(self.all_centers[0]) if self.all_centers else 0
        valid_cols = [True] * feature_count

        # Add weight column
        clustered_data = clustered_data.withColumn("weight", lit(1.0))

        Instance = namedtuple("Instance", ["features", "label", "weight"])
        clustered_rdd = clustered_data.rdd.map(
            lambda row: Instance(DenseVector(row['features']), row['prediction'], row['weight'])
        )

        # Build histogram for splitting
        from d_imm.histogram import DecisionTreeSplitFinder
        split_finder = DecisionTreeSplitFinder(
            num_features=feature_count,
            is_continuous=valid_cols,
            is_unordered=[False] * feature_count,
            max_splits_per_feature=[self.split_count] * feature_count,
            max_bins=self.split_count,
            total_weighted_examples=float(clustered_data.count()),
            seed=42,
            example_count=self.histogram_example_count
        )

        start_time = time.time()
        self.histogram = split_finder.find_splits(input_rdd=clustered_rdd)
        self._log_time(start_time, time.time(), "Time taken to build the histogram", verbose_level=2)
        if self.verbose > 2:
            print("Histogram:", self.histogram)

        # Build the decision tree
        self.tree = self._build_tree(self._to_features_array(clustered_data), valid_centers, valid_cols)
        if self.verbose > 1:
            print("Tree building completed.")

        # Fill node statistics
        start_time = time.time()
        clustered_data = self._to_features_array(clustered_data)
        self.fill_stats_distributed(self.tree, clustered_data)
        self._log_time(start_time, time.time(), "Time taken to fill stats", verbose_level=1)
        return self

    def _build_tree(self, data: DataFrame, valid_centers, valid_cols, depth=0):
        """
        Recursively build the decision tree.
        """
        sample_count = data.count()
        if self.verbose > 2:
            print(f"Building node at depth {depth} with {sample_count} samples")

        node = Node()
        if sample_count == 0:
            node.value = 0
            return node

        if sum(valid_centers) == 1:
            node.value = valid_centers.index(True)
            return node

        unique_labels = data.select("prediction").distinct().count()
        if unique_labels == 1:
            node.value = data.select("prediction").first()[0]
            if self.verbose > 2:
                print(f"Leaf node with label: {node.value}")
            return node

        start_time = time.time()
        split_info = self._find_best_split_distributed_histogram(data, valid_centers, valid_cols)
        self._log_time(start_time, time.time(), "Time taken to find best split", verbose_level=2)

        if not split_info:
            node.value = valid_centers.index(True)
            return node

        # Set split condition
        node.set_condition(split_info['feature'], split_info['threshold'])
        node.mistakes = split_info['mistakes']
        if self.verbose > 2:
            print(f"Splitting on feature {node.feature} at threshold {node.value} with mistakes {node.mistakes}")

        # Ensure features_array is available
        data = self._to_features_array(data)
        left_data = data.filter(col("features_array").getItem(node.feature) <= node.value)
        right_data = data.filter(col("features_array").getItem(node.feature) > node.value)

        left_valid, right_valid = self._update_valid_centers(node.feature, node.value, valid_centers)
        node.left = self._build_tree(left_data, left_valid, valid_cols, depth + 1)
        node.right = self._build_tree(right_data, right_valid, valid_cols, depth + 1)
        return node

    def _find_best_split_distributed_histogram(self, data: DataFrame, valid_centers, valid_cols):
        """
        Find the best split using histogram thresholds in a distributed manner.
        """
        if self.verbose > 2:
            print("Finding best split using histogram thresholds")
        centers_b = self.spark.sparkContext.broadcast(np.array(self.all_centers))
        valid_centers_b = self.spark.sparkContext.broadcast(np.array(valid_centers, dtype=np.int32))
        hist_b = self.spark.sparkContext.broadcast(self.histogram)
        valid_cols_b = self.spark.sparkContext.broadcast(np.array(valid_cols, dtype=np.int32))
        njobs_b = self.spark.sparkContext.broadcast(self.n_jobs)

        def process_partition(iterator):
            import numpy as np
            from d_imm.splitters.cut_finder import get_all_mistakes_histogram
            rows = list(iterator)
            if not rows:
                return []
            features = np.array([row.features for row in rows])
            predictions = np.array([row.prediction for row in rows], dtype=np.int32)
            try:
                results = get_all_mistakes_histogram(
                    features,
                    predictions,
                    centers_b.value,
                    valid_centers_b.value,
                    valid_cols_b.value,
                    hist_b.value,
                    njobs=njobs_b.value
                )
                return results
            except Exception as e:
                print(f"Error in get_all_mistakes_histogram: {e}")
                return []

        start_time = time.time()
        results_rdd = data.rdd.mapPartitions(process_partition)
        all_results = results_rdd.collect()
        self._log_time(start_time, time.time(), "Time to collect worker results", verbose_level=3)

        # Flatten and aggregate results
        flattened = all_results if isinstance(all_results[0], dict) else [r for part in all_results for r in part]
        aggregated = defaultdict(lambda: {'feature': None, 'threshold': None, 'mistakes': 0})
        for res in flattened:
            key = (res['feature'], res['threshold'])
            aggregated[key]['feature'] = res['feature']
            aggregated[key]['threshold'] = res['threshold']
            aggregated[key]['mistakes'] += res['mistakes']

        aggregated_list = list(aggregated.values())
        if not aggregated_list:
            raise ValueError("No valid splits found using histogram thresholds.")
        best = min(aggregated_list, key=lambda x: x['mistakes'])
        if self.verbose > 3:
            print(f"Best split: Feature {best['feature']}, Threshold {best['threshold']}, Mistakes {best['mistakes']}")
        return best

    def _update_valid_centers(self, feature, threshold, valid_centers):
        """Update valid centers for left and right nodes based on the split condition."""
        left_valid = [center and self.centers_broadcast.value[i][feature] <= threshold
                      for i, center in enumerate(valid_centers)]
        right_valid = [center and self.centers_broadcast.value[i][feature] > threshold
                       for i, center in enumerate(valid_centers)]
        return left_valid, right_valid

    def fill_stats_distributed(self, node, data: DataFrame, label_col="prediction"):
        """Fill node statistics in a distributed manner."""
        data = self._to_features_array(data)
        if self._feature_importance is None:
            feature_count = len(data.select("features").first()[0])
            self._feature_importance = [0] * feature_count

        node.samples = data.count()
        if node.is_leaf():
            node.mistakes = data.filter(F.col(label_col) != node.value).count()
        else:
            if node.feature is not None:
                self._feature_importance[node.feature] += 1
            left_data = data.filter(F.col("features_array")[node.feature] <= node.value)
            right_data = data.filter(F.col("features_array")[node.feature] > node.value)
            if node.left:
                self.fill_stats_distributed(node.left, left_data, label_col)
            if node.right:
                self.fill_stats_distributed(node.right, right_data, label_col)

    def feature_importance(self):
        print("Running 'feature_importance' method")
        return self._feature_importance

    def plot(self, filename="test", feature_names=None, view=True):
        """Plot the tree using Graphviz."""
        if not self.tree:
            raise ValueError("Tree has not been built yet.")

        dot_lines = [
            "digraph ClusteringTree {",
            "node [shape=ellipse, style=filled, fillcolor=lightgrey, fontname=Helvetica];"
        ]
        queue = [(self.tree, 0)]
        nodes = []
        edges = []
        while queue:
            current, node_id = queue.pop(0)
            if current.feature is not None:
                feat_name = feature_names[current.feature] if feature_names else current.feature
                label = f"{feat_name} <= {current.value:.3f}\\n" \
                        f"samples={current.samples}\\nmistakes={current.mistakes}"
            else:
                label = f"{current.value}\\n" \
                        f"samples={current.samples}\\nmistakes={current.mistakes}"
            nodes.append((node_id, label))
            if current.left:
                left_id = len(nodes) + len(queue)
                queue.append((current.left, left_id))
                edges.append((node_id, left_id))
            if current.right:
                right_id = len(nodes) + len(queue)
                queue.append((current.right, right_id))
                edges.append((node_id, right_id))
        for nid, label in nodes:
            dot_lines.append(f"n_{nid} [label=\"{label}\"];")
        for parent, child in edges:
            dot_lines.append(f"n_{parent} -> n_{child};")
        dot_lines.append("}")
        dot_str = "\n".join(dot_lines)
        try:
            s = Source(dot_str, filename=filename + '.gv', format="png")
            s.render(view=view)
        except ImportError:
            print("Graphviz not available; DOT file output:")
            print(dot_str)

    def predict(self, x_data: DataFrame):
        """Predict clusters using a Pandas UDF and a broadcasted serialized tree."""
        x_data = self._to_features_array(x_data)
        serialized = serialize_tree(self.tree)
        tree_b = self.spark.sparkContext.broadcast(serialized)

        @pandas_udf(IntegerType())
        def predict_batch(features_batch: pd.Series) -> pd.Series:
            tree = tree_b.value
            return features_batch.apply(lambda features: _predict_subtree(tree, np.array(features)))
        return x_data.withColumn("prediction", predict_batch(col("features_array")))

    def score(self, x_data: DataFrame):
        """Compute k-means cost based on computed cluster means."""
        predicted = self._predict_dataframe(x_data)
        feature_dim = len(self.all_centers[0])
        cluster_means = self._compute_cluster_means(predicted, feature_dim)
        predicted = predicted.join(cluster_means, "prediction")
        @udf(DoubleType())
        def squared_distance(feat, center):
            return float(np.linalg.norm(np.array(feat) - np.array(center)) ** 2)
        cost_df = predicted.withColumn("squared_distance", squared_distance(col("features_array"), col("cluster_mean")))
        return cost_df.agg(F.sum("squared_distance")).collect()[0][0]

    def surrogate_score(self, x_data: DataFrame):
        """Compute surrogate k-means cost using original cluster centers."""
        predicted = self._predict_dataframe(x_data)
        centers_b = self.spark.sparkContext.broadcast(self.all_centers)
        @udf(DoubleType())
        def squared_distance_to_center(feat, cluster):
            center = centers_b.value[cluster]
            return float(np.linalg.norm(np.array(feat) - np.array(center)) ** 2)
        cost_df = predicted.withColumn("squared_distance",
                                       squared_distance_to_center(col("features_array"), col("prediction")))
        return cost_df.agg(F.sum("squared_distance")).collect()[0][0]

    def score_sql(self, x_data: DataFrame):
        """Compute k-means cost using Spark SQL."""
        predicted = self._predict_dataframe(x_data)
        feature_dim = len(self.all_centers[0])
        cluster_means = self._compute_cluster_means(predicted, feature_dim)
        predicted = predicted.join(cluster_means, "prediction")
        joined = self._join_exploded(predicted, "features_array", cluster_means, "cluster_mean")
        total_cost = joined.groupBy("prediction").agg(F.sum("squared_distance").alias("total_cost")) \
            .agg(F.sum("total_cost")).collect()[0][0]
        return total_cost

    def surrogate_score_sql(self, x_data: DataFrame):
        """Compute surrogate k-means cost using Spark SQL and original centers."""
        predicted = self._predict_dataframe(x_data)
        schema = StructType([
            StructField("prediction", IntegerType(), False),
            StructField("center", ArrayType(DoubleType()), False)
        ])
        centers_data = [(i, list(map(float, center))) for i, center in enumerate(self.all_centers)]
        centers_df = self.spark.createDataFrame(centers_data, schema=schema)
        predicted = predicted.join(centers_df, "prediction")
        joined = self._join_exploded(predicted, "features_array", centers_df, "center")
        total_cost = joined.groupBy("prediction").agg(F.sum("squared_distance").alias("total_cost")) \
            .agg(F.sum("total_cost")).collect()[0][0]
        return total_cost
