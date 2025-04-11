package dimm.tree

import dimm.tree.ContinuousSplit

case class Node(
  id: Int,
  depth: Int,
  clusterIds: Set[Int],                      // Clusters represented at this node
  split: Option[ContinuousSplit] = None,     // The chosen split (if any)
  leftChild: Option[Node] = None,
  rightChild: Option[Node] = None,
  isLeaf: Boolean = false                    // True if no further split is needed
) {

  def withSplit(newSplit: ContinuousSplit): Node = {
    this.copy(split = Some(newSplit))
  }

  def withChildren(left: Node, right: Node): Node = {
    this.copy(leftChild = Some(left), rightChild = Some(right))
  }

  def asLeaf(): Node = {
    this.copy(isLeaf = true)
  }

  def isInternal: Boolean = split.isDefined && !isLeaf

  override def toString: String = {
    val label = if (isLeaf) s"Leaf" else s"Node $id"
    val splitStr = split.map(_.toString).getOrElse("No split")
    val clusters = clusterIds.mkString(",")
    s"$label: depth=$depth, clusters=[$clusters], split=[$splitStr]"
  }
}
