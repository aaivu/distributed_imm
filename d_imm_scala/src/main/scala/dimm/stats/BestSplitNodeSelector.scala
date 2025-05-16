package dimm.stats

object BestSplitPerNodeSelector {

  def selectBestPerNode(
      statsMap: Map[(Int, Int), NodeFeatureStats]
  ): Map[Int, BestSplitDecision] = {

    statsMap
      .filter(_._2.bestSplit.isDefined)
      .groupBy { case ((nodeId, _), _) => nodeId }
      .map { case (nodeId, entries) =>
        val ((_, featureIndex), bestStat) = entries.minBy(_._2.bestSplitMistakes)
        nodeId -> BestSplitDecision(
          nodeId = bestStat.nodeId,
          featureIndex = featureIndex,
          split = bestStat.bestSplit.get,
          mistakeCount = bestStat.bestSplitMistakes
        )
      }
  }
}