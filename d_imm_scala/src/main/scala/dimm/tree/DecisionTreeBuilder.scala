package dimm.tree

import dimm.core.BinnedInstance
import dimm.stats.BestSplitDecision
import org.apache.spark.rdd.RDD

object TreeBuilder {

  def updateTree(
      currentTree: Map[Int, Node],
      bestSplits: Map[Int, BestSplitDecision],
      updatedInstances: RDD[BinnedInstance]
  ): Map[Int, Node] = {

    // Step 1: Get clusterIds assigned to each node after the split
    val clusterAssignments = updatedInstances
      .filter(_.isValid)
      .map(inst => (inst.nodeId, inst.clusterId))
      .distinct()
      .groupByKey()
      .mapValues(_.toSet)
      .collect()
      .toMap

    // Step 2: Build updated nodes
    var newTree = currentTree

    for ((nodeId, decision) <- bestSplits) {
      val parentNode = currentTree(nodeId)

      val leftChildId = nodeId * 2 + 1
      val rightChildId = nodeId * 2 + 2

      val leftClusters = clusterAssignments.getOrElse(leftChildId, Set.empty)
      val rightClusters = clusterAssignments.getOrElse(rightChildId, Set.empty)

      val leftChild = Node(
        id = leftChildId,
        depth = parentNode.depth + 1,
        clusterIds = leftClusters,
        isLeaf = leftClusters.size <= 1
      )

      val rightChild = Node(
        id = rightChildId,
        depth = parentNode.depth + 1,
        clusterIds = rightClusters,
        isLeaf = rightClusters.size <= 1
      )

      val updatedParent = parentNode
        .withSplit(decision.split)
        .withChildren(leftChild, rightChild)

      newTree += (nodeId -> updatedParent)
      newTree += (leftChildId -> leftChild)
      newTree += (rightChildId -> rightChild)
    }

    newTree
  }
}
