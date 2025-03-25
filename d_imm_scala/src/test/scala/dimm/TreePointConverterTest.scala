package dimm

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD

class TreePointConverterTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("TreePointConverterTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("TreePointConverter correctly bins Iris dataset features") {
    val irisRDD = loadIrisAsInstances("test_data/iris_stacked.csv")

    val metadata = new DecisionTreeMetadata(
      numFeatures = 4,
      numSplits = 3,
      weightedNumExamples = irisRDD.count().toDouble,
      maxBins = 32,
      numExamples = irisRDD.count(),
      isContinuous = _ => true
    )

    val splits = SplitFinder.findSplits(irisRDD, metadata, seed = 42L)

    val treePoints = TreePointConverter.convertToTreeRDD(irisRDD, splits, metadata)

    // Print a few samples for visual verification
    treePoints.take(5).foreach { tp =>
      println(s"label=${tp.label}, binnedFeatures=${tp.binnedFeatures.mkString(",")}, weight=${tp.weight}")
    }

    // Basic checks
    assert(treePoints.count() == irisRDD.count())
    assert(treePoints.take(1).head.binnedFeatures.length == 4)
  }

  def loadIrisAsInstances(path: String): RDD[Instance] = {
    val df = spark.read.option("header", "true").csv(path)

    df.rdd.map { row =>
      val features = Vectors.dense(
        row.getAs[String]("sepal length (cm)").toDouble,
        row.getAs[String]("sepal width (cm)").toDouble,
        row.getAs[String]("petal length (cm)").toDouble,
        row.getAs[String]("petal width (cm)").toDouble
      )
      Instance(label = 0.0, weight = 1.0, features = features)
    }
  }
}
