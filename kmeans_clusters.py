import sys
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, DoubleType

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kmeans_job.py <input_csv_gcs_path> <output_gcs_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("KMeans GCS Job").getOrCreate()

    # Load CSV from GCS
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    # Drop first column
    columns_to_use = df.columns
    df = df.select(columns_to_use)

    # Assemble feature vector
    assembler = VectorAssembler(inputCols=columns_to_use, outputCol="features")
    assembled = assembler.transform(df)

    # Train KMeans model
    kmeans = KMeans().setK(10).setSeed(1)
    model = kmeans.fit(assembled)
    transformed = model.transform(assembled)

    # Print cluster centers
    print("Cluster Centers:")
    for center in model.clusterCenters():
        print(center)


    spark.stop()
