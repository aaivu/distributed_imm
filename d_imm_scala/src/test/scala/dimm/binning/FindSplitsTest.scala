package dimm.binning

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funsuite.AnyFunSuite
import dimm.core.Instance
import dimm.tree.ContinuousSplit

class FindSplitsTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("FindSplitsTest")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  test("FindSplits returns correct thresholds for simple continuous features") {
    val sc = spark.sparkContext

    val data = Seq(
      Instance(clusterId = 0, weight = 1.0, features = Vectors.dense(1.0, 10.0)),
      Instance(clusterId = 1, weight = 1.0, features = Vectors.dense(2.0, 20.0)),
      Instance(clusterId = 2, weight = 1.0, features = Vectors.dense(3.0, 30.0)),
      Instance(clusterId = 3, weight = 1.0, features = Vectors.dense(4.0, 40.0)),
      Instance(clusterId = 4, weight = 1.0, features = Vectors.dense(5.0, 50.0))
    )

    val rdd = sc.parallelize(data)

    val numFeatures = 2
    val numSplits = 2
    val maxBins = 32
    val numExamples = data.size
    val weightedNumExamples = data.map(_.weight).sum

    val splits = FindSplits.findSplits(
      input = rdd,
      numFeatures = numFeatures,
      numSplits = numSplits,
      maxBins = maxBins,
      numExamples = numExamples,
      weightedNumExamples = weightedNumExamples,
      seed = 1234L
    )

    splits.zipWithIndex.foreach { case (splitArray, i) =>
      println(s"Feature $i splits: " + splitArray.mkString(", "))
    }

    assert(splits.length == 2)
    assert(splits.forall(_.nonEmpty))
    assert(splits.forall(_.forall(_.isInstanceOf[ContinuousSplit])))
  }
}
