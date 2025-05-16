package dimm.driver

import dimm.core.{IMMRunner, Instance}
import dimm.tree.TreePrinter
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.rdd.RDD

// import org.apache.log4j.{Level, Logger}
// Logger.getLogger("org").setLevel(Level.ERROR)       // Suppress INFO/WARN from Spark internals
// Logger.getLogger("akka").setLevel(Level.ERROR)

object IMMJob {
  def main(args: Array[String]): Unit = {
    


    println("Received args: " + args.mkString(", "))
    
    // === Parse Arguments ===
    val argMap = args.sliding(2, 2).map { case Array(flag, value) => flag -> value }.toMap
    val k = argMap.getOrElse("--k", "3").toInt
    val dataPath = argMap("--data")
    val master = argMap.getOrElse("--master", "local[*]")

    println(s"Running IMM with k=$k on $dataPath using master='$master'")

    // val spark = SparkSession.builder()
    //   .appName("IMM GCP Job")
    //   .master(master)
    //   .getOrCreate()

    val spark = SparkSession.builder()
      .appName("IMM GCP Job")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    import spark.implicits._

    // === Load Dataset ===
    val rawDF = spark.read.option("header", "true").csv(dataPath)
    val featureCols = rawDF.columns
    val typedDF = featureCols.foldLeft(rawDF)((df, col) => df.withColumn(col, df(col).cast("double")))
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val featureDF = assembler.transform(typedDF).select("features")

    // === KMeans Clustering ===
    val kmeans = new KMeans().setK(k).setSeed(42)
    val model = kmeans.fit(featureDF)
    val clustered = model.transform(featureDF).select("features", "prediction")

    val clusteredInstances: RDD[Instance] = clustered.rdd.map {
      case Row(features: Vector, clusterId: Int) =>
        Instance(clusterId, 1.0, features)
    }.cache()

    // === Time IMM Execution ===
    val start = System.nanoTime()
    val (tree, splits) = IMMRunner.runIMM(clusteredInstances, model.clusterCenters, numSplits = 10, maxBins = 32)
    val end = System.nanoTime()

    val seconds = (end - start).toDouble / 1e9
    println(f"\n=== IMM Completed in $seconds%.3f seconds ===")

    // === Print Tree ===
    TreePrinter.printTree(tree, splits)

    spark.stop()
  }
}
