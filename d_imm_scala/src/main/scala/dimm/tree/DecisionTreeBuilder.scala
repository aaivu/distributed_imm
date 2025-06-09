package dimm.tree

import dimm.core.BinnedInstance
import dimm.stats.BestSplitDecision
import org.apache.spark.rdd.RDD

object TreeBuilder {

  def updateTree(
      currentTree: Map[Int, Node],
      bestSplits: Map[Int, BestSplitDecision],
      updatedInstances: RDD[BinnedInstance],
      parentStats: Map[Int, (Int, Int)],
      centerBinLookup: Map[(Int, Int), Int]
  ): Map[Int, Node] = {

    // Step 1: Compute (nodeId → (sampleCount, mistakeCount)) for updated instances
    val nodeStats: Map[Int, (Int, Int)] = updatedInstances
      .map(inst => (inst.nodeId, (1, if (!inst.isValid) 1 else 0)))
      .reduceByKey { case ((aCount, aMistakes), (bCount, bMistakes)) =>
        (aCount + bCount, aMistakes + bMistakes)
      }
      .collect()
      .toMap

    // Step 2: Build lookup from clusterId → binnedFeatures (used for splitting clusters)
    val clusterBinLookup = centerBinLookup

    // Step 3: Update tree with split info
    var newTree = currentTree

    for ((nodeId, decision) <- bestSplits) {
      val parentNode = currentTree(nodeId)

      val leftChildId = nodeId * 2 + 1
      val rightChildId = nodeId * 2 + 2

      val parentClusters = parentNode.clusterIds
      val featureIndex = decision.split.featureIndex
      val threshold = decision.split.threshold

      // Partition parent clusters based on their binned feature value
      val (leftClusters, rightClusters) = parentClusters.partition { clusterId =>
        clusterBinLookup.getOrElse((clusterId, featureIndex), Int.MaxValue) <= threshold
      }


      // Stats per child node
      val leftStats = nodeStats.getOrElse(leftChildId, (0, 0))
      val rightStats = nodeStats.getOrElse(rightChildId, (0, 0))

      // Build child nodes
      val leftChild = Node(
        id = leftChildId,
        depth = parentNode.depth + 1,
        clusterIds = leftClusters,
        isLeaf = leftClusters.size <= 1
      ).withSampleStats(leftStats._1, leftStats._2)

      val rightChild = Node(
        id = rightChildId,
        depth = parentNode.depth + 1,
        clusterIds = rightClusters,
        isLeaf = rightClusters.size <= 1
      ).withSampleStats(rightStats._1, rightStats._2)

      // Parent stats before split
      val thisParentStats = parentStats.getOrElse(nodeId, (0, 0))

      val updatedParent = parentNode
        .withSplit(decision.split)
        .withChildren(leftChild, rightChild)
        .withSampleStats(thisParentStats._1, thisParentStats._2)

      // Update tree
      newTree += (nodeId -> updatedParent)
      newTree += (leftChildId -> leftChild)
      newTree += (rightChildId -> rightChild)
    }

    newTree
  }
}
