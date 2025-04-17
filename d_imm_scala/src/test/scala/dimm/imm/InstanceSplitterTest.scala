package dimm.split

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import dimm.core.{BinnedInstance, BinnedCenter}
import dimm.tree.ContinuousSplit
import dimm.stats.BestSplitDecision

class InstanceSplitterTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("InstanceSplitterTest")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  test("updateInstances correctly reassigns nodes and marks mistakes") {

    val instances: RDD[BinnedInstance] = sc.parallelize(Seq(
      BinnedInstance(Array(1, 2), nodeId = 0, clusterId = 0, weight = 1.0, isValid = true), // LEFT (correct)
      BinnedInstance(Array(3, 2), nodeId = 0, clusterId = 0, weight = 1.0, isValid = true), // RIGHT (mistake)
      BinnedInstance(Array(3, 2), nodeId = 0, clusterId = 1, weight = 1.0, isValid = true)  // RIGHT (correct)
    ))

    val centers = Map(
      0 -> BinnedCenter(Array(1, 2), clusterId = 0), // Center 0 is LEFT of threshold
      1 -> BinnedCenter(Array(4, 2), clusterId = 1)  // Center 1 is RIGHT of threshold
    )

    val bestSplits = Map(
      0 -> BestSplitDecision(
        nodeId = 0,
        featureIndex = 0,
        split = ContinuousSplit(0, threshold = 2.0),
        mistakeCount = 1
      )
    )

    val (updated, _) = InstanceSplitter.updateInstances(
      instances,
      bestSplits,
      centerLookup = centers,
      nodeIdCounterStart = 2
    )

    val collected = updated.collect().sortBy(_.binnedFeatures(0))

    val inst1 = collected(0) // Goes LEFT (bin 1), same as center → valid
    val inst2 = collected(1) // Goes RIGHT (bin 3), center was LEFT → mistake
    val inst3 = collected(2) // Goes RIGHT (bin 3), center was RIGHT → valid

    assert(inst1.nodeId == 1)  // left child of node 0
    assert(inst1.isValid)

    assert(inst2.nodeId == 2)  // right child of node 0
    assert(!inst2.isValid)     // mistake

    assert(inst3.nodeId == 2)
    assert(inst3.isValid)
  }
}
