import sys
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, DoubleType
import os

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kmeans_job.py <input_csv_gcs_path> <output_gcs_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("KMeans GCS Job").getOrCreate()

    # Load CSV from GCS
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    # Use all columns
    columns_to_use = df.columns
    df = df.select(columns_to_use)

    # Assemble feature vector
    assembler = VectorAssembler(inputCols=columns_to_use, outputCol="features")
    assembled = assembler.transform(df)

    # Train KMeans model
    kmeans = KMeans().setK(10).setSeed(42)
    model = kmeans.fit(assembled)
    transformed = model.transform(assembled)

    # Print and save cluster centers
    centers = model.clusterCenters()
    print("Cluster Centers:")
    for center in centers:
        print(center)

    # Save to local temporary text file
    local_cluster_path = "/tmp/cluster_centers.txt"
    with open(local_cluster_path, "w") as f:
        f.write(str([center.tolist() for center in centers]))

    # Upload the file to GCS using gsutil
    output_cluster_path = output_path + "_cluster"
    os.system(f"gsutil cp {local_cluster_path} {output_cluster_path}")

    # Convert features vector to array
    vector_to_array = udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType()))
    flattened = transformed.withColumn("features_array", vector_to_array("features"))

    # Expand features array into individual columns
    num_features = len(columns_to_use)
    feature_columns = [
        col("features_array")[i].alias(f"f{i}") for i in range(num_features)
    ]
    final_df = flattened.select(*feature_columns, col("prediction"))

    # Write to GCS as CSV
    final_df.write.mode("overwrite").option("header", True).csv(output_path)

    spark.stop()
