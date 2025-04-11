package dimm.stats

import dimm.tree.ContinuousSplit

case class NodeFeatureStats(
  nodeId: Int,
  featureIndex: Int,
  clusterBinCounts: Map[Int, Map[Int, Double]],  // clusterId -> (binIndex -> count)
  clusterMinBin: Int,
  clusterMaxBin: Int,
  bestSplit: Option[ContinuousSplit] = None,
  bestSplitMistakes: Long = Long.MaxValue
) {

  def isSplittable: Boolean = clusterMinBin < clusterMaxBin

  def updateBestSplit(split: ContinuousSplit, mistakeCount: Long): NodeFeatureStats = {
    if (mistakeCount < bestSplitMistakes)
      this.copy(bestSplit = Some(split), bestSplitMistakes = mistakeCount)
    else
      this
  }

  override def toString: String = {
    val splitStr = bestSplit.map(_.toString).getOrElse("No split")
    s"Node $nodeId - Feature $featureIndex: MinBin=$clusterMinBin, MaxBin=$clusterMaxBin, BestSplit=$splitStr, Mistakes=$bestSplitMistakes"
  }
}
