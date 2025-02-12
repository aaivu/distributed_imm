from pyspark.ml.clustering import KMeans as SparkKMeans
from pyspark.ml.linalg import DenseVector
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.ml.functions import vector_to_array
from collections import defaultdict
from pyspark.sql.functions import col, lit
from collections import namedtuple

from d_imm.histogram import DecisionTreeSplitFinder
import numpy as np
import time

from d_imm.splitters import get_all_mistakes

try:
    from graphviz import Source

    graphviz_available = True
except Exception:
    graphviz_available = False


class Node:
    def __init__(self):
        self.feature = None
        self.value = None
        self.samples = None
        self.mistakes = None
        self.left = None
        self.right = None
        # TODO: Didn't check the BASE_TREE. Must check for max leaves condition if exkmc is to be implemented.

    def is_leaf(self):
        return (self.left is None) and (self.right is None)

    def set_condition(self, feature, value):
        self.feature = feature
        self.value = value


Split = namedtuple("Split", ["feature_index", "threshold", "categories", "is_continuous"])


class DistributedIMM:
    def __init__(self, spark, k, max_leaves=None, verbose=0, n_jobs=None, mode=0, example_count=10000, split_count=32):
        """
        Initialize the DistributedIMM class with Spark session and parameters.

        :param spark: SparkSession instance.
        :param k: Number of clusters.
        :param max_leaves: Maximum number of leaves allowed in the tree.
        :param verbose: Verbosity level.
        :param n_jobs: The number of jobs to run in parallel.
        """
        self.spark = spark
        self.k = k
        self.tree = None
        self.max_leaves = max_leaves if max_leaves else k
        self.verbose = verbose
        self.n_jobs = n_jobs if n_jobs else 1
        self._feature_importance = None
        self.nodes = []
        self.mode = mode
        self.split_count = split_count
        self.histogram_example_count = example_count
        self.histogram = None
        

    def fit(self, x_data: DataFrame, kmeans_model=None):
        """
        Build a threshold tree from the training set x_data.
        :param x_data: The training input samples as a Spark DataFrame.
        :param kmeans_model: Pre-trained KMeans model (optional).
        :return: Fitted threshold tree.
        """
        if self.verbose > 0:
            print("Running 'fit' method")

        # TODO: Include KMedians
        # Cluster data and prepare labeled dataset
        if kmeans_model is None:
            if self.verbose > 0:
                print('Training kmeans with %d clusters' % self.k)
            kmeans = SparkKMeans().setK(self.k).setSeed(1).setMaxIter(40).setFeaturesCol("features")
            kmeans_model = kmeans.fit(x_data)
        else:
            assert kmeans_model.getK() == self.k, "Provided KMeans model must have the same number of clusters as 'k'"
            kmeans_model = kmeans_model

        # Get predictions and cluster centers
        clustered_data = kmeans_model.transform(x_data).select("features", "prediction")

        # Get cluster centers as a Python list and broadcast it
        self.all_centers = kmeans_model.clusterCenters()
        # TODO: NOT SURE IF THE FOLLOWING BROADCAST IS REALLY NEEDED. CENTERS ARE BROADCASTED INSIDE _find_best_split_distributed FUNCTION ALSO. CHECK IF THIS IS NEEDED.
        self.centers_broadcast = self.spark.sparkContext.broadcast(self.all_centers)

        if self.verbose > 3:
            print("Cluster centers:", self.all_centers)
            print("Sample of clustered data:")
            clustered_data.show(5)

        valid_centers = [True] * self.k
        feature_count = len(self.all_centers[0]) if self.all_centers else 0
        valid_cols = [True] * feature_count

        # Add a weight column to the DataFrame (default weight = 1.0)
        clustered_data_with_weight = clustered_data.withColumn("weight", lit(1.0))

        # # Convert clustered_data DataFrame to an RDD with the required structure
        # clustered_rdd = clustered_data_with_weight.rdd.map(
        #     lambda row: (DenseVector(row['features']), row['prediction'], row['weight'])
        # )

        Instance = namedtuple("Instance", ["features", "label", "weight"])

        clustered_rdd = clustered_data_with_weight.rdd.map(
            lambda row: Instance(DenseVector(row['features']), row['prediction'], row['weight'])
        )

        # Initialize the root node and start building the tree
        start_time = time.time()
        split_finder = DecisionTreeSplitFinder(
            num_features=feature_count,
            is_continuous=valid_cols,
            is_unordered=[False for _ in valid_cols],
            max_splits_per_feature=[self.split_count] * feature_count,  # Default Value = 32
            max_bins=self.split_count,  # Default Value = 32
            total_weighted_examples=float(clustered_data_with_weight.count()),  # Use count for total weight
            seed=42,  # Default Value = 42
            example_count=self.histogram_example_count
        )

        # Find splits using the split finder
        start_time = time.time()
        self.histogram = split_finder.find_splits(input_rdd=clustered_rdd)
        end_time = time.time()

        if self.verbose > 2:
            elapsed_time = (end_time - start_time)
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Time taken to build the histogram: {int(minutes)} minutes and {seconds:.2f} seconds")
            print("Histogram:", self.histogram)
        
        # self.histogram_broadcast = self.spark.sparkContext.broadcast(self.histogram)

        self.tree = self._build_tree(clustered_data, valid_centers=valid_centers, valid_cols=valid_cols)
        end_time = time.time()

        if self.verbose > 1:
            elapsed_time = (end_time - start_time)
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Time taken to build the tree: {int(minutes)} minutes and {seconds:.2f} seconds")

        # TODO: use vector_to_array before the build_tree method
        clustered_data_vector = clustered_data.withColumn("features_array", vector_to_array("features"))

        if self.verbose > 0:
            print("Running '__fill_stats_distributed__' method")
        start_time = time.time()
        self.fill_stats_distributed(self.tree, clustered_data_vector)
        end_time = time.time()

        if self.verbose > 1:
            elapsed_time = (end_time - start_time)
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Time taken to fill stats: {int(minutes)} minutes and {seconds:.2f} seconds")


        if self.verbose > 0:
            print("Tree building completed.")

        return self

    def _build_tree(self, x_data_with_y, valid_centers, valid_cols, depth=0):
        """
        Recursively build the decision tree.

        :param x_data_with_y: DataFrame containing features and cluster predictions.
        :param valid_centers: List of boolean flags indicating valid cluster centers.
        :param valid_cols: List of boolean flags indicating valid columns for splitting.
        :param depth: Current depth of the tree (used to check max_leaves).
        :return: Root node of the constructed subtree.
        """
        # Count the number of samples in the current node
        sample_count = x_data_with_y.count()
        if self.verbose > 2:
            print(f"Building node at depth {depth} with {sample_count} samples")

        node = Node()

        # Check stopping conditions
        if sample_count == 0:
            node.value = 0
            return node

        if sum(valid_centers) == 1:
            node.value = valid_centers.index(True)  # Assign single valid center label
            return node

        unique_labels = x_data_with_y.select("prediction").distinct().count()
        if unique_labels == 1:
            single_label = x_data_with_y.select("prediction").first()[0]
            node.value = single_label
            if self.verbose > 2:
                print(f"Node is a leaf with cluster label: {single_label}")
            return node

        start_time = time.time()
        if self.mode == 0:
            split_info = self._find_best_split(x_data_with_y, valid_centers, valid_cols)
        elif self.mode == 1:
            split_info = self._find_best_split_distributed(x_data_with_y, valid_centers, valid_cols)
        elif self.mode == 2:
            split_info = self._find_best_split_distributed_histogram(x_data_with_y, valid_centers, valid_cols)
        elif self.mode == 3:
            split_info = self._find_best_split_distributed_histogram(x_data_with_y, valid_centers, valid_cols, sorted=False)
        end_time = time.time()

        if self.verbose > 2:
            elapsed_time = (end_time - start_time)
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Time taken to find the best split: {int(minutes)} minutes and {seconds:.2f} seconds")

        if not split_info:
            # Default to the most frequent valid center if no split is found
            node.value = valid_centers.index(True)
            return node

        # Extract split details
        feature = split_info['feature']
        threshold = split_info['threshold']
        mistakes = split_info['mistakes']

        node.set_condition(feature, threshold)
        # Store mistakes directly in the node - may work for histograms
        node.mistakes = mistakes

        if self.verbose > 2:
            print(f"Splitting on feature {feature} at threshold {threshold} with mistakes {mistakes}")

        # Divide data into left and right nodes
        # Convert `features` column to array if not already done
        if "features_array" not in x_data_with_y.columns:
            x_data_with_y = x_data_with_y.withColumn("features_array", vector_to_array(col("features")))

        start_time = time.time()
        left_data = x_data_with_y.filter(col("features_array").getItem(feature) <= threshold)
        right_data = x_data_with_y.filter(col("features_array").getItem(feature) > threshold)
        end_time = time.time()
        if self.verbose > 2:
            elapsed_time = (end_time - start_time)
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Time taken to filter left and right data: {int(minutes)} minutes and {seconds:.2f} seconds")

        start_time = time.time()
        left_valid_centers, right_valid_centers = self._update_valid_centers(feature, threshold, valid_centers)
        end_time = time.time()
        if self.verbose > 2:
            elapsed_time = (end_time - start_time)
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Time taken to update centers: {int(minutes)} minutes and {seconds:.2f} seconds")

        # Recursively build left and right child nodes
        node.left = self._build_tree(left_data, left_valid_centers, valid_cols, depth + 1)
        node.right = self._build_tree(right_data, right_valid_centers, valid_cols, depth + 1)

        return node

    def _find_best_split_distributed(self, x_data, valid_centers, valid_cols):
        """
        Find the best split for a single node in a distributed fashion using Spark.
        :param x_data: Spark DataFrame with features and cluster predictions.
        :param valid_centers: List of valid cluster centers.
        :param valid_cols: List of valid columns for splitting.
        :return: Dictionary with keys 'feature', 'threshold', and 'mistakes'.
        """
        if self.verbose > 2:
            print("Finding the best split in a distributed manner")

        # Broadcast the centers, valid_centers, and valid_cols
        centers_broadcast = self.spark.sparkContext.broadcast(np.array(self.all_centers))
        valid_centers_broadcast = self.spark.sparkContext.broadcast(np.array(valid_centers, dtype=np.int32))
        valid_cols_broadcast = self.spark.sparkContext.broadcast(np.array(valid_cols, dtype=np.int32))

        def process_partition(iterator):
            """
            Function to process a partition of the data.
            """
            import numpy as np
            from d_imm.splitters.cut_finder import get_all_mistakes

            # TODO: CHECK IF THIS IS EFFICIENT. Convert the iterator to a list of rows
            rows = list(iterator)
            if not rows:
                return []

            features = np.array([row.features for row in rows])
            predictions = np.array([row.prediction for row in rows], dtype=np.int32)

            try:
                results = get_all_mistakes(
                    features,
                    predictions,
                    centers_broadcast.value,
                    valid_centers_broadcast.value,
                    valid_cols_broadcast.value,
                    njobs=1
                )
                return results
            except Exception as e:
                print(f"Error in get_all_mistakes: {e}")
                return []

        start_time = time.time()
        # Apply the function to each partition
        results_rdd = x_data.rdd.mapPartitions(process_partition)

        # Collect and aggregate results
        all_results = results_rdd.collect()

        end_time = time.time()
        if self.verbose > 3:
            elapsed_time = (end_time - start_time)
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Time taken to collect results from worker nodes:{int(minutes)} minutes and {seconds:.2f} seconds")

        start_time = time.time()
        if isinstance(all_results[0], dict):
            flattened_results = all_results
        else:
            # Otherwise, flatten the list of lists
            flattened_results = [result for partition_results in all_results for result in partition_results]

        if self.verbose > 3:
            print(f"Flattened results: {flattened_results}")

        # Aggregate mistakes for the same 'feature' and 'threshold'
        aggregated_results = defaultdict(lambda: {'feature': None, 'threshold': None, 'mistakes': 0})

        for result in flattened_results:
            key = (result['feature'], result['threshold'])
            aggregated_results[key]['feature'] = result['feature']
            aggregated_results[key]['threshold'] = result['threshold']
            aggregated_results[key]['mistakes'] += result['mistakes']

        # Convert aggregated results back to a list
        aggregated_results_list = list(aggregated_results.values())

        if self.verbose > 3:
            print(f"Aggregated results: {aggregated_results_list}")

        # Find the best split (minimum mistakes)
        best_split = min(aggregated_results_list, key=lambda x: x['mistakes'])

        end_time = time.time()
        if self.verbose > 3:
            elapsed_time = (end_time - start_time)
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Time taken to aggregate results:{int(minutes)} minutes and {seconds:.2f} seconds")

        if self.verbose > 3:
            print(
                f"Best split found: Feature {best_split['feature']}, "
                f"Threshold {best_split['threshold']}, Mistakes {best_split['mistakes']}"
            )

        return {'feature': best_split['feature'], 'threshold': best_split['threshold'],
                'mistakes': best_split['mistakes']}

    def _find_best_split_distributed_histogram(self, x_data, valid_centers, valid_cols, sorted=True):
        """
        Find the best split for a single node using histogram thresholds in a distributed fashion.
        :param x_data: Spark DataFrame with features and cluster predictions.
        :param valid_centers: List of valid cluster centers.
        :param histograms: Precomputed histograms for each feature (list of lists of Split objects).
        :return: Dictionary with keys 'feature', 'threshold', and 'mistakes'.
        """
        if self.verbose > 2:
            print("Finding the best split using histogram thresholds in a distributed manner")

        # Broadcast the centers, valid_centers, and histograms
        centers_broadcast = self.spark.sparkContext.broadcast(np.array(self.all_centers))
        valid_centers_broadcast = self.spark.sparkContext.broadcast(np.array(valid_centers, dtype=np.int32))
        histograms_broadcast = self.spark.sparkContext.broadcast(self.histogram)
        valid_cols_broadcast = self.spark.sparkContext.broadcast(np.array(valid_cols, dtype=np.int32))

        def process_partition(iterator):
            """
            Function to process a partition of the data.
            """
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
                    centers_broadcast.value,
                    valid_centers_broadcast.value,
                    valid_cols_broadcast.value,
                    histograms_broadcast.value,
                    njobs=1,
                    sorted=sorted
                )
                return results
            except Exception as e:
                print(f"Error in get_all_mistakes_histogram: {e}")
                return []

        start_time = time.time()
        results_rdd = x_data.rdd.mapPartitions(process_partition)
        all_results = results_rdd.collect()
        end_time = time.time()

        if self.verbose > 3:
            elapsed_time = (end_time - start_time)
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Time taken to collect results from worker nodes: {int(minutes)} minutes and {seconds:.2f} seconds")

        start_time = time.time()
        if isinstance(all_results[0], dict):
            flattened_results = all_results
        else:
            # Otherwise, flatten the list of lists
            flattened_results = [result for partition_results in all_results for result in partition_results]

        if self.verbose > 3:
            print(f"Flattened results: {flattened_results}")

        # Aggregate mistakes for the same 'feature' and 'threshold'
        aggregated_results = defaultdict(lambda: {'feature': None, 'threshold': None, 'mistakes': 0})

        for result in flattened_results:
            key = (result['feature'], result['threshold'])
            aggregated_results[key]['feature'] = result['feature']
            aggregated_results[key]['threshold'] = result['threshold']
            aggregated_results[key]['mistakes'] += result['mistakes']

        # Convert aggregated results back to a list
        aggregated_results_list = list(aggregated_results.values())

        if self.verbose > 3:
            print(f"Aggregated results: {aggregated_results_list}")

        if not aggregated_results_list:
            raise ValueError("No valid splits found using histogram thresholds.")

        # Find the best split (minimum mistakes)
        best_split = min(aggregated_results_list, key=lambda x: x['mistakes'])

        end_time = time.time()
        if self.verbose > 3:
            elapsed_time = (end_time - start_time)
            minutes, seconds = divmod(elapsed_time, 60)
            print(f"Time taken to aggregate results: {int(minutes)} minutes and {seconds:.2f} seconds")

        if self.verbose > 3:
            print(
                f"Best split found: Feature {best_split['feature']}, "
                f"Threshold {best_split['threshold']}, Mistakes {best_split['mistakes']}"
            )

        return {'feature': best_split['feature'], 'threshold': best_split['threshold'],
                'mistakes': best_split['mistakes']}

    def _find_best_split(self, x_data, valid_centers, valid_cols):
        """
        Find the best split on a single node (driver) using the broadcasted Cython function.
        """
        if self.verbose > 2:
            print("Finding the best split on the driver node")

        # Broadcast the centers and Cython function
        centers = np.array(self.all_centers)

        # Prepare the valid centers and valid columns arrays
        valid_centers_np = np.array(valid_centers, dtype=np.int32)
        valid_cols_np = np.array(valid_cols, dtype=np.int32)

        # Collect all data to the driver node
        collected_data = x_data.collect()

        # Convert collected data to NumPy arrays
        features = np.array([row.features for row in collected_data])
        predictions = np.array([row.prediction for row in collected_data], dtype=np.int32)

        if self.verbose > 3:
            print(f"Data collected: {features.shape[0]} samples, {features.shape[1]} features")

        # Call the Cython function directly on the driver node
        try:
            results = get_all_mistakes(
                features, predictions, centers, valid_centers_np, valid_cols_np, njobs=1
            )
        except Exception as e:
            print(f"Error running get_all_mistakes: {e}")
            raise

        # Find the best split directly from results
        best_split = min(results, key=lambda x: x['mistakes'])

        if self.verbose > 3:
            print(f"Results: {results}")
            print(
                f"Best split found: Feature {best_split['feature']}, Threshold {best_split['threshold']}, Mistakes {best_split['mistakes']}")

        return {'feature': best_split['feature'], 'threshold': best_split['threshold'],
                'mistakes': best_split['mistakes']}

    def _update_valid_centers(self, feature, threshold, valid_centers):
        """
        Update valid centers for left and right nodes based on the chosen split.

        :param feature: Feature index used for splitting.
        :param threshold: Threshold value for the split.
        :param valid_centers: List of valid cluster centers.
        :return: Tuple (left_valid_centers, right_valid_centers)
        """
        # Adjust valid centers based on split for left and right nodes
        left_valid_centers = [center and self.centers_broadcast.value[i][feature] <= threshold for i, center in
                              enumerate(valid_centers)]
        right_valid_centers = [center and self.centers_broadcast.value[i][feature] > threshold for i, center in
                               enumerate(valid_centers)]

        return left_valid_centers, right_valid_centers

    def fill_stats_distributed(self, node, clustered_data: DataFrame, label_col="prediction"):
        """
        Distributed version of __fill_stats__ for large datasets using PySpark.
        Uses clustered_data which includes both vectorized features and labels.

        :param node: Current node of the tree.
        :param clustered_data: Spark DataFrame containing 'features' and cluster labels ('prediction').
        :param label_col: Column name containing the cluster labels.
        """

        # Dynamically initialize feature importance if it's None
        if self._feature_importance is None:
            feature_count = len(clustered_data.select("features").first()[0])
            self._feature_importance = [0] * feature_count

        # Total samples in the current node
        node.samples = clustered_data.count()

        # Process leaf nodes
        if node.is_leaf():
            # Count mistakes: rows where the label does not match the node's value
            node.mistakes = clustered_data.filter(F.col(label_col) != node.value).count()
        else:
            # Update feature importance
            if hasattr(self, '_feature_importance') and node.feature is not None:
                self._feature_importance[node.feature] += 1

            # Split data into left and right based on feature index and threshold
            left_data = clustered_data.filter(F.col("features_array")[node.feature] <= node.value)
            right_data = clustered_data.filter(F.col("features_array")[node.feature] > node.value)

            # Recursively process left and right children
            if node.left:
                self.fill_stats_distributed(node.left, left_data, label_col)
            if node.right:
                self.fill_stats_distributed(node.right, right_data, label_col)

    def feature_importance(self):
        print("Running 'feature_importance' method")
        return self._feature_importance

    def plot(self, filename="test", feature_names=None, view=True):
        """
        Plot the tree using Graphviz with a cleaner style.
        :param filename: Name for the output file.
        :param feature_names: Names of features for labeling.
        :param view: Whether to view the plot after saving.
        """
        if not self.tree:
            raise ValueError("Tree has not been built yet.")

        dot_str = ["digraph ClusteringTree {\n"]
        dot_str.append("node [shape=ellipse, style=filled, fillcolor=lightgrey, fontname=Helvetica];\n")

        queue = [(self.tree, 0)]  # Node with its unique ID
        nodes = []
        edges = []

        while queue:
            current, node_id = queue.pop(0)

            # Prepare label
            if current.feature is not None:
                label = (
                    f"{feature_names[current.feature] if feature_names else current.feature} "
                    f"<= {current.value:.3f}\\n"
                    f"samples={current.samples}\\n"
                    f"mistakes={current.mistakes}"
                )
            else:  # Leaf node
                label = (
                    f"{current.value}\\n"
                    f"samples={current.samples}\\n"
                    f"mistakes={current.mistakes}"
                )
            nodes.append((node_id, label))

            # Add children to the queue
            if current.left:
                left_id = len(nodes) + len(queue)  # Unique ID
                queue.append((current.left, left_id))
                edges.append((node_id, left_id))
            if current.right:
                right_id = len(nodes) + len(queue)  # Unique ID
                queue.append((current.right, right_id))
                edges.append((node_id, right_id))

        # Convert nodes and edges into dot format for Graphviz
        for node_id, label in nodes:
            dot_str.append(f"n_{node_id} [label=\"{label}\"];\n")
        for parent, child in edges:
            dot_str.append(f"n_{parent} -> n_{child};\n")
        dot_str.append("}")

        dot_str = "".join(dot_str)

        try:
            from graphviz import Source
            s = Source(dot_str, filename=filename + '.gv', format="png")
            s.render(view=view)
        except ImportError:
            print("Graphviz not available; outputting DOT file as plain text.")
            print(dot_str)
