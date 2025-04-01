package dimm.tree

import dimm.core.{Instance, ClusterCenter}

case class Node(
  id: Int,
  depth: Int,
  points: Array[Instance],
  centers: Array[ClusterCenter],
  split: Option[ContinuousSplit] = None,
  isLeaf: Boolean = false,
  mistakes: Int = 0,
  left: Option[Node] = None,
  right: Option[Node] = None
) {

  def numClusters: Int = centers.map(_.clusterId).distinct.length

  def hasNoClusters: Boolean = centers.isEmpty

  def shouldStopSplitting: Boolean = isLeaf || numClusters <= 1 || hasNoClusters

  def withSplit(
    split: ContinuousSplit,
    leftChild: Node,
    rightChild: Node,
    mistakes: Int
  ): Node = {
    this.copy(
      split = Some(split),
      isLeaf = false,
      mistakes = mistakes,
      left = Some(leftChild),
      right = Some(rightChild)
    )
  }

  def markAsLeaf(): Node = {
    this.copy(isLeaf = true)
  }

  override def toString: String = {
    val splitStr = split.map(_.toString).getOrElse("Leaf")
    s"Node(id=$id, depth=$depth, split=$splitStr, points=${points.length}, clusters=${centers.length}, mistakes=$mistakes)"
  }
}
