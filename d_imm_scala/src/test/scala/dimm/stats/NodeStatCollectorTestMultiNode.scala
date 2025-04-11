package dimm.stats

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import dimm.core.{BinnedInstance, BinnedCenter}
import dimm.tree.ContinuousSplit

class NodeStatsCollectorMultiNodeTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("NodeStatsCollectorMultiNodeTest")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  test("NodeStatsCollector computes correct stats for multiple nodes") {

    val instances: RDD[BinnedInstance] = sc.parallelize(Seq(
      // Node 0, Cluster 0
      BinnedInstance(Array(0, 1), 0, 0, 1.0, true),
      BinnedInstance(Array(1, 2), 0, 0, 1.0, true),

      // Node 0, Cluster 1
      BinnedInstance(Array(2, 3), 0, 1, 1.0, true),
      BinnedInstance(Array(3, 2), 0, 1, 1.0, true),

      // Node 1, Cluster 0
      BinnedInstance(Array(1, 0), 1, 0, 1.0, true),
      BinnedInstance(Array(2, 1), 1, 0, 1.0, true),

      // Node 1, Cluster 1
      BinnedInstance(Array(3, 3), 1, 1, 1.0, true),
      BinnedInstance(Array(2, 2), 1, 1, 1.0, true)
    ))

    val centers: RDD[BinnedCenter] = sc.parallelize(Seq(
      BinnedCenter(Array(1, 2), 0),  // Cluster 0 center
      BinnedCenter(Array(3, 3), 1)   // Cluster 1 center
    ))

    val dummySplits: Array[Array[ContinuousSplit]] = Array.fill(2)(Array.empty)

    val statsMap = NodeStatsCollector.computeStats(instances, centers, dummySplits)

    // Check expected keys
    val expectedKeys = Set((0, 0), (0, 1), (1, 0), (1, 1))
    assert(statsMap.keySet == expectedKeys)

    // Validate Node 0, Feature 0 stats
    val node0F0 = statsMap((0, 0))
    assert(node0F0.clusterBinCounts(0)(0) == 1.0)
    assert(node0F0.clusterBinCounts(0)(1) == 1.0)
    assert(node0F0.clusterBinCounts(1)(2) == 1.0)
    assert(node0F0.clusterBinCounts(1)(3) == 1.0)
    assert(node0F0.clusterMinBin == 1)
    assert(node0F0.clusterMaxBin == 3)

    // Validate Node 1, Feature 1 stats
    val node1F1 = statsMap((1, 1))
    assert(node1F1.clusterBinCounts(0)(0) == 1.0)
    assert(node1F1.clusterBinCounts(0)(1) == 1.0)
    assert(node1F1.clusterBinCounts(1)(2) == 1.0)
    assert(node1F1.clusterBinCounts(1)(3) == 1.0)
    assert(node1F1.clusterMinBin == 2)
    assert(node1F1.clusterMaxBin == 3)

    println("Node 0, Feature 0:\n" + node0F0)
    println("Node 1, Feature 1:\n" + node1F1)
  }
}
