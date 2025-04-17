package dimm.stats

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import dimm.core.{BinnedInstance, BinnedCenter}
import dimm.tree.ContinuousSplit

class NodeStatsCollectorTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("NodeStatsCollectorTest")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  test("NodeStatsCollector computes correct stats for 2 clusters and 2 features") {
    val binnedInstances: RDD[BinnedInstance] = sc.parallelize(Seq(
      // Cluster 0 (bin 0s and 1s)
      BinnedInstance(Array(0, 1), 0, 0, 1.0, true),
      BinnedInstance(Array(1, 1), 0, 0, 1.0, true),
      BinnedInstance(Array(1, 1), 0, 0, 1.0, true),
      BinnedInstance(Array(0, 0), 0, 0, 1.0, true),
      BinnedInstance(Array(0, 0), 0, 0, 1.0, true),
      BinnedInstance(Array(1, 0), 0, 0, 1.0, true),
      BinnedInstance(Array(0, 1), 0, 0, 1.0, true),

      // Cluster 1 (bin 2s and 3s)
      BinnedInstance(Array(2, 2), 0, 1, 1.0, true),
      BinnedInstance(Array(3, 2), 0, 1, 1.0, true),
      BinnedInstance(Array(3, 3), 0, 1, 1.0, true),
      BinnedInstance(Array(2, 2), 0, 1, 1.0, true),
      BinnedInstance(Array(3, 2), 0, 1, 1.0, true),
      BinnedInstance(Array(2, 3), 0, 1, 1.0, true),
      BinnedInstance(Array(3, 2), 0, 1, 1.0, true),
      BinnedInstance(Array(3, 3), 0, 1, 1.0, true)
    ))

    val binnedCenters: RDD[BinnedCenter] = sc.parallelize(Seq(
      BinnedCenter(Array(1, 1), 0),
      BinnedCenter(Array(3, 2), 1)
    ))

    val dummySplits: Array[Array[ContinuousSplit]] = Array.fill(2)(Array.empty)

    val statsMap = NodeStatsCollector.computeStats(binnedInstances, binnedCenters, dummySplits)

    // There should be stats for 2 features (0, 1) at node 0
    assert(statsMap.contains((0, 0)))
    assert(statsMap.contains((0, 1)))

    val f0Stats = statsMap((0, 0))
    val f1Stats = statsMap((0, 1))

    // Cluster 0 should use bins 0 and 1
    assert(f0Stats.clusterMinBin == 1)
    assert(f0Stats.clusterMaxBin == 3)
    assert(f1Stats.clusterMinBin == 1)
    assert(f1Stats.clusterMaxBin == 2)

    // Check that cluster 0 has multiple bin counts in feature 0
    val cluster0BinsF0 = f0Stats.clusterBinCounts(0)
    assert(cluster0BinsF0(0) == 4.0)
    assert(cluster0BinsF0(1) == 3.0)

    // Check that cluster 1 has bin counts in feature 1
    val cluster1BinsF1 = f1Stats.clusterBinCounts(1)
    assert(cluster1BinsF1(2) == 5.0)
    assert(cluster1BinsF1(3) == 3.0)

    println("Feature 0 Stats: " + f0Stats)
    println("Feature 1 Stats: " + f1Stats)
  }
}
