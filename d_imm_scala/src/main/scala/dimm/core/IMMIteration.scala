package dimm.core

import dimm.binning.FindSplits
import dimm.core.{BinnedCenter, BinnedInstance}
import dimm.stats.{NodeStatsCollector, SplitEvaluator, BestSplitPerNodeSelector, BestSplitDecision}
import dimm.split.InstanceSplitter
import dimm.tree.{Node, TreeBuilder}
import org.apache.spark.rdd.RDD

object IMMIteration {

  def runIteration(
      instances: RDD[BinnedInstance],
      centers: RDD[BinnedCenter],
      tree: Map[Int, Node],
      splits: Array[Array[dimm.tree.ContinuousSplit]],
      nodeIdCounter: Int
  ): (RDD[BinnedInstance], Map[Int, Node], Int, Boolean) = {

    // Step 1: Compute node-feature stats
    val nodeFeatureStats = NodeStatsCollector.computeStats(instances, centers, splits)
    val centerBinLookup: Map[(Int, Int), Int] = centers
      .flatMap(center =>
        center.binnedFeatures.zipWithIndex.map {
          case (bin, featureIndex) => ((center.clusterId, featureIndex), bin)
        }
      )
      .collect()
      .toMap

    // Step 2: Evaluate best split per feature
    val statsWithBestSplits = SplitEvaluator.evaluate(nodeFeatureStats,centerBinLookup)

    // Step 3: Pick best feature-split per node
    val bestSplitMap: Map[Int, BestSplitDecision] = BestSplitPerNodeSelector.selectBestPerNode(statsWithBestSplits)
    // Step 3.5: Compute parent node stats before splitting
    val parentNodeStats: Map[Int, (Int, Int)] = instances
      .filter(inst => inst.isValid && bestSplitMap.contains(inst.nodeId))
      .map(inst => (inst.nodeId, (1, 0)))
      .reduceByKey((a, b) => (a._1 + b._1, 0)) // No mistake count needed here
      .collect()
      .toMap

    // === Print best split details per node ===
    println("=== Best Split Decisions Per Node ===")
    bestSplitMap.foreach { case (nodeId, decision) =>
      println(s"Node $nodeId: featureIndex=${decision.featureIndex}, threshold=${decision.split.threshold}, mistakes=${decision.mistakeCount}")
    }
    println("======================================")

    if (bestSplitMap.isEmpty) {
      println("No more splittable nodes.")
      return (instances, tree, nodeIdCounter, true)
    }

    // Step 4: Reassign instances + mark mistakes
    val centerMap = centers.map(c => c.clusterId -> c).collect().toMap
    val (updatedInstances, nextNodeIdCounter) =
      InstanceSplitter.updateInstances(instances, bestSplitMap, centerMap, nodeIdCounter)

    // Step 5: Update tree structure
    val updatedTree = TreeBuilder.updateTree(tree, bestSplitMap, updatedInstances, parentNodeStats, centerBinLookup)


    (updatedInstances, updatedTree, nextNodeIdCounter, false)
  }
}