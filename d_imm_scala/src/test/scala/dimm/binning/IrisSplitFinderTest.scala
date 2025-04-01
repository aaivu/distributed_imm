package dimm.binning

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import dimm.core.Instance
import dimm.tree.ContinuousSplit

class IrisSplitFinderTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("IrisSplitFinderTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("FindSplits works correctly on Iris dataset") {
    val irisRDD = loadIrisAsInstances("test_data/iris_stacked.csv")

    val numFeatures = 4
    val numSplits = 10
    val maxBins = 32
    val numExamples = irisRDD.count().toInt
    val weightedNumExamples = irisRDD.map(_.weight).sum().toDouble

    val splits = FindSplits.findSplits(
      input = irisRDD,
      numFeatures = numFeatures,
      numSplits = numSplits,
      maxBins = maxBins,
      numExamples = numExamples,
      weightedNumExamples = weightedNumExamples,
      seed = 42L
    )

    splits.zipWithIndex.foreach { case (splitArr, i) =>
      println(s"Feature $i splits: ${splitArr.mkString(", ")}")
    }

    assert(splits.length == 4)
    assert(splits.forall(_.nonEmpty))
    assert(splits.forall(_.forall(_.isInstanceOf[ContinuousSplit])))
  }

  def loadIrisAsInstances(path: String): RDD[Instance] = {
    val df = spark.read
      .option("header", "true")
      .csv(path)

    df.rdd.map { row =>
      val features = Vectors.dense(
        row.getString(0).toDouble,
        row.getString(1).toDouble,
        row.getString(2).toDouble,
        row.getString(3).toDouble
      )
      Instance(
        clusterId = -1,  // assuming cluster not yet assigned
        weight = 1.0,
        features = features
      )
    }
  }
}
