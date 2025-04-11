import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import dimm.core.Instance
import dimm.tree.ContinuousSplit
import dimm.binning.FindSplits
import org.scalatest.funsuite.AnyFunSuite

class FindSplitsTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("FindSplitsTest")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext

  test("findSplits includes cluster center values and returns sorted thresholds") {

    val data: RDD[Instance] = sc.parallelize(Seq(
      Instance(clusterId = 0, weight = 1.0, features = Vectors.dense(1.0, 3.0)),
      Instance(clusterId = 0, weight = 1.0, features = Vectors.dense(2.0, 2.0)),
      Instance(clusterId = 1, weight = 1.0, features = Vectors.dense(4.0, 1.0)),
      Instance(clusterId = 1, weight = 1.0, features = Vectors.dense(5.0, 4.0))
    ))

    val clusterCenters: Array[Vector] = Array(
      Vectors.dense(1.5, 3.0),  // Center 0
      Vectors.dense(4.5, 2.5)   // Center 1
    )

    val splits: Array[Array[ContinuousSplit]] = FindSplits.findSplits(
      input = data,
      clusterCenters = clusterCenters,
      numFeatures = 2,
      numSplits = 2,
      maxBins = 5,
      numExamples = 4,
      weightedNumExamples = 4.0,
      seed = 42L
    )

    // Check 1: All splits are sorted for each feature
    splits.foreach { featureSplits =>
      val thresholds = featureSplits.map(_.threshold)
      assert(thresholds.sameElements(thresholds.sorted),
        s"Thresholds not sorted: ${thresholds.mkString(", ")}")
    }

    // Check 2: Cluster center values are included
    val centerFeature0Vals = clusterCenters.map(_(0)).toSet
    val splitThresholdsFeature0 = splits(0).map(_.threshold).toSet

    centerFeature0Vals.foreach { centerVal =>
      assert(
        splitThresholdsFeature0.exists(t => math.abs(t - centerVal) < 1e-6),
        s"Expected cluster center value $centerVal in feature 0 splits"
      )
    }
  }
}
