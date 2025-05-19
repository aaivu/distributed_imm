package dimm.tree

import org.apache.spark.ml.linalg.Vector

object TreeRouter {

  /**
   * Traverse the decision tree to find the clusterId associated with the leaf node
   * that this feature vector is routed to.
   */
  def findLeaf(features: Vector, tree: Map[Int, Node]): Int = {
    var currentId = 0

    while (!tree(currentId).isLeaf) {
      val node = tree(currentId)
      node.split match {
        case Some(split) =>
          val featureValue = features(split.featureIndex)
          val goLeft = featureValue <= split.threshold
          currentId = if (goLeft) currentId * 2 + 1 else currentId * 2 + 2
        case None =>
          // Defensive: No split info — treat as leaf
          return tree(currentId).clusterIds.head
      }
    }

    // Leaf node reached → return clusterId it isolates
    tree(currentId).clusterIds.head
  }
}
