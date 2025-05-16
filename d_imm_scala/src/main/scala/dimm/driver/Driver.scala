// package dimm.driver

// // import dimm.binning.{FindSplits, BinAndAssign}
// import dimm.core._
// import dimm.tree.{Node, TreePrinter}

// import org.apache.spark.ml.clustering.KMeans
// import org.apache.spark.ml.feature.VectorAssembler
// import org.apache.spark.ml.linalg.{Vectors, Vector}
// import org.apache.spark.sql.{SparkSession, Row}
// import org.apache.spark.rdd.RDD

// object IMMDriver {

//   def main(args: Array[String]): Unit = {
//     val spark = SparkSession.builder()
//       .appName("Distributed IMM")
//       .master("local[*]")  // Change this to your Spark cluster config if needed
//       .getOrCreate()

//     import spark.implicits._

//     // === Step 1: Load dataset ===
//     val path = if (args.nonEmpty) args(0) else "data/iris.csv"
//     val rawDF = spark.read.option("header", "true").csv(path)

//     // 1.1: Cast all columns to double (assumes all columns are numeric)
//     val featureCols = rawDF.columns
//     val typedDF = featureCols.foldLeft(rawDF) { (df, col) =>
//       df.withColumn(col, df(col).cast("double"))
//     }

//     // 1.2: Assemble vector for KMeans
//     val assembler = new VectorAssembler()
//       .setInputCols(featureCols)
//       .setOutputCol("features")

//     val featureDF = assembler.transform(typedDF).select("features")

//     // === Step 2: Run KMeans ===
//     val k = 4  // Change if needed
//     val kmeans = new KMeans().setK(k).setSeed(42)
//     val model = kmeans.fit(featureDF)
//     val clustered = model.transform(featureDF).select("features", "prediction")

//     // === Step 3: Convert to RDD[Instance] ===
//     val clusteredInstances: RDD[Instance] = clustered.rdd.map {
//       case Row(features: Vector, clusterId: Int) =>
//         Instance(clusterId, 1.0, features)
//     }.cache()

//     val clusterCenters: Array[Vector] = model.clusterCenters

//     val startTime = System.nanoTime()

//     val (tree, splits) = IMMRunner.runIMM(clusteredInstances, clusterCenters)
//     val endTime = System.nanoTime()
//     val elapsedSeconds = (endTime - startTime).toDouble / 1e9
//     println(f"\n=== IMM Completed in $elapsedSeconds%.3f seconds ===")
//     TreePrinter.printTree(tree, splits)
    

    
//     spark.stop()
//   }
// }

