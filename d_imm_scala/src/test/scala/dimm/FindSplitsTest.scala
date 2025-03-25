package dimm

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.tree._
// import org.apache.spark.ml.feature.Instance
import dimm.Instance
class SplitFinderTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("SplitFinderTest")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  test("SplitFinder returns expected splits for continuous features") {
    val sc = spark.sparkContext

    val data = Seq(
      Instance(0.0, 1.0, Vectors.dense(1.0, 2.0)),
      Instance(0.0, 1.0, Vectors.dense(2.0, 3.0)),
      Instance(0.0, 1.0, Vectors.dense(3.0, 4.0)),
      Instance(0.0, 1.0, Vectors.dense(4.0, 5.0)),
      Instance(0.0, 1.0, Vectors.dense(5.0, 6.0))
    )

    val rdd = sc.parallelize(data)

    val metadata = new DecisionTreeMetadata(
      numFeatures = 2,
      numSplits = 2,
      weightedNumExamples = data.map(_.weight).sum,
      maxBins = 32,
      numExamples = data.size,
      isContinuous = _ => true // all features are continuous
    )

    val splits = SplitFinder.findSplits(rdd, metadata, seed = 42L)

    // Print for visual confirmation
    splits.zipWithIndex.foreach { case (splitArr, i) =>
      println(s"Feature $i: ${splitArr.mkString(", ")}")
    }

    assert(splits.length == 2)
    assert(splits.forall(_.nonEmpty))
  }
}
