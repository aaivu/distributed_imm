package dimm.driver

import dimm.binning.{FindSplits, BinAndAssign}
import dimm.core._
import dimm.tree.Node
import dimm.core.IMMIteration
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.rdd.RDD

object IMMDriver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Distributed IMM")
      .master("local[*]")  // Change this to your Spark cluster config if needed
      .getOrCreate()

    import spark.implicits._

    // === Step 1: Load dataset ===
    val path = if (args.nonEmpty) args(0) else "data/iris.csv"
    val rawDF = spark.read.option("header", "true").csv(path)

    // 1.1: Cast all columns to double (assumes all columns are numeric)
    val featureCols = rawDF.columns
    val typedDF = featureCols.foldLeft(rawDF) { (df, col) =>
      df.withColumn(col, df(col).cast("double"))
    }

    // 1.2: Assemble vector for KMeans
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    val featureDF = assembler.transform(typedDF).select("features")

    // === Step 2: Run KMeans ===
    val k = 3  // Change if needed
    val kmeans = new KMeans().setK(k).setSeed(42)
    val model = kmeans.fit(featureDF)
    val clustered = model.transform(featureDF).select("features", "prediction")

    // === Step 3: Convert to RDD[Instance] ===
    val clusteredInstances: RDD[Instance] = clustered.rdd.map {
      case Row(features: Vector, clusterId: Int) =>
        Instance(clusterId, 1.0, features)
    }.cache()

    val clusterCenters: Array[Vector] = model.clusterCenters

    // === Step 4: Compute Splits ===
    val numFeatures = clusterCenters.head.size
    val numExamples = clusteredInstances.count().toInt
    val weightedNumExamples = clusteredInstances.map(_.weight).sum().toDouble

    val splits = FindSplits.findSplits(
      input = clusteredInstances,
      clusterCenters = clusterCenters,
      numFeatures = numFeatures,
      numSplits = 10,
      maxBins = 32,
      numExamples = numExamples,
      weightedNumExamples = weightedNumExamples,
      seed = 42L
    )

    // === Step 5: Bin Instances and Centers ===
    val binnedInstances = BinAndAssign.binInstancesRDD(clusteredInstances, splits).cache()
    val binnedCenters = BinAndAssign.binClusterCentersRDD(clusterCenters, splits).cache()

    // === Step 6: Initialize Tree ===
    val allClusters = clusteredInstances.map(_.clusterId).distinct().collect().toSet
    var tree = Map(0 -> Node(id = 0, depth = 0, clusterIds = allClusters))
    var currentInstances = binnedInstances
    var nodeCounter = 1
    var done = false

    // === Step 7: IMM Iteration Loop ===
    while (!done) {
      val (nextInstances, nextTree, nextCounter, converged) =
        IMMIteration.runIteration(currentInstances, binnedCenters, tree, splits, nodeCounter)

      currentInstances = nextInstances
      tree = nextTree
      nodeCounter = nextCounter
      done = converged
    }

    // === Step 8: Print Final Tree ===
    println("\n=== Final IMM Explanation Tree ===")
    tree.toSeq.sortBy(_._1).foreach { case (id, node) =>
      println(s"Node $id: $node")
    }

    spark.stop()
  }
}
