package dimm.split

import dimm.core.{BinnedInstance, BinnedCenter}
import dimm.stats.BestSplitDecision
import dimm.tree.ContinuousSplit
import org.apache.spark.rdd.RDD

object InstanceSplitter {

  def updateInstances(
      instances: RDD[BinnedInstance],
      bestSplits: Map[Int, BestSplitDecision],
      centerLookup: Map[Int, BinnedCenter],
      nodeIdCounterStart: Int
  ): (RDD[BinnedInstance], Int) = {

    val broadcastSplits = instances.context.broadcast(bestSplits)
    val broadcastCenters = instances.context.broadcast(centerLookup)

    var nextNodeId = nodeIdCounterStart

    val newInstances = instances.map { instance =>
      if (!instance.isValid || !broadcastSplits.value.contains(instance.nodeId)) {
        // Leave invalid or non-split nodes unchanged
        instance
      } else {
        val splitDecision = broadcastSplits.value(instance.nodeId)
        val split = splitDecision.split
        val bin = instance.binnedFeatures(split.featureIndex)

        val goLeft = bin <= split.threshold

        // Assign new nodeId: left or right child (generate deterministically)
        val leftChildId = instance.nodeId * 2 + 1
        val rightChildId = instance.nodeId * 2 + 2
        val newNodeId = if (goLeft) leftChildId else rightChildId

        // Determine where the cluster center lies
        val centerBin = broadcastCenters.value(instance.clusterId).binnedFeatures(split.featureIndex)
        val centerGoesLeft = centerBin <= split.threshold

        val isMistake = goLeft != centerGoesLeft

        instance.copy(
          nodeId = newNodeId,
          isValid = !isMistake
        )
      }
    }

    // Return updated instances and the next node ID counter (for building Nodes)
    val updatedMaxId = newInstances.map(_.nodeId).max()
    (newInstances, math.max(updatedMaxId + 1, nextNodeId))
  }
}
