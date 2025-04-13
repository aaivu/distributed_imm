package dimm.stats

object BestSplitPerNodeSelector {

  def selectBestPerNode(
      statsMap: Map[(Int, Int), NodeFeatureStats]
  ): Map[Int, BestSplitDecision] = {

    statsMap
      .filter(_._2.bestSplit.isDefined)
      .groupBy { case ((nodeId, _), _) => nodeId }
      .mapValues { entries =>
        val ((_, featureIndex), bestStat) = entries.minBy(_._2.bestSplitMistakes)
        BestSplitDecision(
          nodeId = bestStat.nodeId,
          featureIndex = featureIndex,
          split = bestStat.bestSplit.get,
          mistakeCount = bestStat.bestSplitMistakes
        )
      }
      .toMap
  }
}
