from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, pandas_udf, lit
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import numpy as np
import datetime
import pandas as pd
# import json

# Tree node structure
class Node:
    def __init__(self):
        self.feature = None
        self.value = None
        self.samples = None
        self.mistakes = None
        self.left = None
        self.right = None

def serialize_tree(node):
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

def build_tree(data, feature_cols, depth=0, max_depth=5):
    node = Node()
    sample_count = data.limit(1).count()
    if sample_count == 0:
        return None

    unique_predictions = data.select('prediction').distinct().count()
    if unique_predictions == 1 or depth >= 10:  # Maximum depth
        node.value = data.groupBy('prediction').count().orderBy('count', ascending=False).first()['prediction']
        return node

    best_split = find_best_split(data, feature_cols)

    if best_split is None:
        node.value = data.groupBy('prediction').count().orderBy('count', ascending=False).first()['prediction']
        return node

    node.feature = best_split['feature']
    node.value = best_split['threshold']
    node.mistakes = best_split['mistakes']

    left_data = data.filter(col(node.feature) <= node.value).cache()
    right_data = data.filter(col(node.feature) > node.value).cache()

    node.left = build_tree(left_data, depth + 1)
    node.right = build_tree(right_data, depth + 1)

    left_data.unpersist()
    right_data.unpersist()

    return node

def find_best_split(data, feature_cols):
    min_mistakes = float('inf')
    best_split = None

    for feature in feature_cols:
        thresholds = data.approxQuantile(feature, [0.25, 0.5, 0.75], 0.01)
        for thresh in thresholds:
            left_mistakes = data.filter((col(feature) <= thresh) & (col('prediction') != 0)).count()
            right_mistakes = data.filter((col(feature) > thresh) & (col('prediction') != 1)).count()
            mistakes = left_mistakes + right_mistakes

            if best_split is None or mistakes < best_split['mistakes']:
                best_split = {'feature': feature, 'threshold': thresh, 'mistakes': mistakes}

    return best_split

# def predict(data, serialized_tree):
#     @pandas_udf(IntegerType())
#     def predict_udf(features):
#         def _predict(node, features):
#             if node['left'] is None and node['right'] is None:
#                 return node['value']
#             if features[node['feature']] <= node['value']:
#                 return _predict(node['left'], features)
#             else:
#                 return _predict(node['right'], features)
#         return features.apply(lambda x: _predict(serialized_tree, x))

#     return data.withColumn('prediction', predict_udf(col('features')))

# Main execution example
if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName(f"GCS_Data") \
        .config("spark.executor.instances", 6) \
        .config("spark.executor.cores", 4) \
        .getOrCreate()

    sc = spark.sparkContext

    # Configuration
    N_CLUSTERS = 10
    GCS_BUCKET_NAME = "d-imm-test"  # Remove 'gs://' here
    output_folder = "images"
    time_start = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    datasets = [1000]
    i = 1000

    # Define input file path in GCS
    input_file = f"gs://{GCS_BUCKET_NAME}/sub_data_{i}000.csv"
    # Read the CSV file into a PySpark DataFrame
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    feature_cols = df.columns  # Assuming last column is target or ignored
    assembler = udf(lambda *cols: [float(c) for c in cols], "array<double>")
    df = df.withColumn("features", assembler(*feature_cols))

    # Run KMeans clustering
    kmeans = KMeans(k=10, seed=42, featuresCol="features")
    model = kmeans.fit(df)
    clustered_data = model.transform(df).select(*feature_cols, 'features', 'prediction').cache()
    print(model.clusterCenters())
    # Build Decision Tree IMM
    start_time = datetime.datetime.now()
    tree = build_tree(clustered_data, feature_cols, max_depth=4)
    end_time = datetime.datetime.now()
    print("Training time:", (end_time - start_time).total_seconds(), "seconds")
    spark.stop()
