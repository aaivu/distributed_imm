package dimm

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD

class IrisSplitFinderTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("IrisSplitFinderTest")
    .master("local[*]")
    .getOrCreate()

  test("SplitFinder works correctly on the Iris dataset") {
    val irisRDD = loadIrisAsInstances("test_data/iris_stacked.csv")

    val metadata = new DecisionTreeMetadata(
      numFeatures = 4,
      numSplits = 10,
      weightedNumExamples = irisRDD.count().toDouble,
      maxBins = 32,
      numExamples = irisRDD.count(),
      isContinuous = _ => true
    )

    val splits = SplitFinder.findSplits(irisRDD, metadata, seed = 42L)

    splits.zipWithIndex.foreach { case (splitArr, i) =>
      println(s"Feature $i splits: ${splitArr.mkString(", ")}")
    }

    assert(splits.length == 4)
    assert(splits.forall(_.nonEmpty))
  }

  def loadIrisAsInstances(path: String): RDD[Instance] = {
    val df = spark.read.option("header", "true").csv(path)

    val instanceRDD = df.rdd.map { row =>
      val features = Vectors.dense(
        row.getString(0).toDouble,
        row.getString(1).toDouble,
        row.getString(2).toDouble,
        row.getString(3).toDouble
      )
      Instance(label = 0.0, weight = 1.0, features = features)
    }

    instanceRDD
  }
}
