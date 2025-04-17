package dimm.tree

import dimm.tree.ContinuousSplit

case class Node(
  id: Int,
  depth: Int,
  clusterIds: Set[Int],
  split: Option[ContinuousSplit] = None,
  leftChild: Option[Node] = None,
  rightChild: Option[Node] = None,
  isLeaf: Boolean = false,
  samples: Option[Int] = None,
  mistakes: Option[Int] = None
) {

  def withSplit(newSplit: ContinuousSplit): Node = {
    this.copy(split = Some(newSplit))
  }

  def withChildren(left: Node, right: Node): Node = {
    this.copy(leftChild = Some(left), rightChild = Some(right))
  }

  def withSampleStats(samples: Int, mistakes: Int): Node = {
    this.copy(samples = Some(samples), mistakes = Some(mistakes))
  }

  def isInternal: Boolean = split.isDefined && !isLeaf

  def splitStringWithRealThreshold(splits: Array[Array[ContinuousSplit]]): String = {
    split match {
      case Some(s) =>
        val featureSplits = splits(s.featureIndex)
        val binIndex = s.threshold.toInt
        if (binIndex < featureSplits.length)
          f"(f${s.featureIndex} <= ${featureSplits(binIndex).threshold}%.3f)"
        else
          s"(f${s.featureIndex} <= [unknown])"
      case None => "No split"
    }
  }

  override def toString: String = {
    val label = if (isLeaf) s"Leaf" else s"Node $id"
    val splitStr = split.map(_.toString).getOrElse("No split")
    val clusters = clusterIds.mkString(",")
    val sampleStr = samples.map(s => s", samples=$s").getOrElse("")
    val mistakeStr = mistakes.map(m => s", mistakes=$m").getOrElse("")
    s"$label: depth=$depth, clusters=[$clusters], split=[$splitStr]$sampleStr$mistakeStr"
  }
}
