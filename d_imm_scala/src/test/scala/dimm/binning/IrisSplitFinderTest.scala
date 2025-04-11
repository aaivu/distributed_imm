package dimm.binning

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import dimm.core.Instance
import dimm.tree.ContinuousSplit

class IrisSplitFinderTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("IrisSplitFinderTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("FindSplits works correctly on Iris dataset including cluster centers") {
    val irisRDD = loadIrisAsInstances("test_data/iris_stacked.csv")

    val numFeatures = 4
    val numSplits = 10
    val maxBins = 32
    val numExamples = irisRDD.count().toInt
    val weightedNumExamples = irisRDD.map(_.weight).sum().toDouble

    // Placeholder cluster centers for test (real values should come from KMeans)
    val clusterCenters: Array[Vector] = Array(
      Vectors.dense(5.0, 3.5, 1.4, 0.2),
      Vectors.dense(6.0, 2.9, 4.5, 1.5),
      Vectors.dense(6.5, 3.0, 5.5, 2.0)
    )

    val splits = FindSplits.findSplits(
      input = irisRDD,
      clusterCenters = clusterCenters, // 🔁 ADDED
      numFeatures = numFeatures,
      numSplits = numSplits,
      maxBins = maxBins,
      numExamples = numExamples,
      weightedNumExamples = weightedNumExamples,
      seed = 42L
    )

    splits.zipWithIndex.foreach { case (splitArr, i) =>
      println(s"Feature $i splits: ${splitArr.mkString(", ")}")

      // Check that splits are sorted
      val thresholds = splitArr.map(_.threshold)
      assert(thresholds.sameElements(thresholds.sorted),
        s"Feature $i splits are not sorted.")
    }

    assert(splits.length == 4)
    assert(splits.forall(_.nonEmpty))
    assert(splits.forall(_.forall(_.isInstanceOf[ContinuousSplit])))

    // Check that center values are included as splits
    for (i <- 0 until numFeatures) {
      val splitThresholds = splits(i).map(_.threshold).toSet
      val centerFeatureVals = clusterCenters.map(_(i)).toSet
      centerFeatureVals.foreach { v =>
        assert(
          splitThresholds.exists(t => math.abs(t - v) < 1e-6),
          s"Expected cluster center value $v in feature $i splits"
        )
      }
    }
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
