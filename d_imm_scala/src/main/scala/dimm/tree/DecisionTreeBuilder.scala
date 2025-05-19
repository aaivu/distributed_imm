package dimm.tree

import dimm.core.BinnedInstance
import dimm.stats.BestSplitDecision
import org.apache.spark.rdd.RDD

object TreeBuilder {

  def updateTree(
      currentTree: Map[Int, Node],
      bestSplits: Map[Int, BestSplitDecision],
      updatedInstances: RDD[BinnedInstance],
      parentStats: Map[Int, (Int, Int)] // NEW
  ): Map[Int, Node] = {

    // Step 1: Group stats per nodeId: (samples, mistakes)
    val nodeStats: Map[Int, (Int, Int)] = updatedInstances
      .map(inst => (inst.nodeId, (1, if (!inst.isValid) 1 else 0)))
      .reduceByKey((a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2))
      .collect()
      .toMap

    // Step 2: Get clusterIds assigned to each node
    val clusterAssignments = updatedInstances
      .filter(_.isValid)
      .map(inst => (inst.nodeId, inst.clusterId))
      .distinct()
      .groupByKey()
      .mapValues(_.toSet)
      .collect()
      .toMap

    // Step 3: Build updated nodes
    var newTree = currentTree

    for ((nodeId, decision) <- bestSplits) {
      val parentNode = currentTree(nodeId)

      val leftChildId = nodeId * 2 + 1
      val rightChildId = nodeId * 2 + 2

      val leftClusters = clusterAssignments.getOrElse(leftChildId, Set.empty)
      val rightClusters = clusterAssignments.getOrElse(rightChildId, Set.empty)

      val leftStats = nodeStats.getOrElse(leftChildId, (0, 0))
      val rightStats = nodeStats.getOrElse(rightChildId, (0, 0))

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

      val thisParentStats = parentStats.getOrElse(nodeId, (0, 0))

      val updatedParent = parentNode
        .withSplit(decision.split)
        .withChildren(leftChild, rightChild)
        .withSampleStats(thisParentStats._1, thisParentStats._2)

      newTree += (nodeId -> updatedParent)
      newTree += (leftChildId -> leftChild)
      newTree += (rightChildId -> rightChild)
    }

    newTree
  }
}
