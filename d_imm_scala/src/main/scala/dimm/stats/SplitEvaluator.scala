package dimm.stats

import dimm.tree.ContinuousSplit

object SplitEvaluator {

  def evaluate(statsMap: Map[(Int, Int), NodeFeatureStats]): Map[(Int, Int), NodeFeatureStats] = {
    statsMap.map { case (key, stats) =>
      key -> findBestSplit(stats)
    }
  }

  def findBestSplit(stats: NodeFeatureStats): NodeFeatureStats = {
    if (!stats.isSplittable) return stats

    val minBin = stats.clusterMinBin
    val maxBin = stats.clusterMaxBin

    var bestStats = stats
    for (splitBin <- (minBin + 1) to (maxBin - 1)) {
      val split = ContinuousSplit(stats.featureIndex, threshold = splitBin.toDouble)

      val mistakeCount = computeMistakes(
        splitBin,
        stats.clusterBinCounts,
        centerBinLookup = estimateClusterCenterBins(stats)
      )

      bestStats = bestStats.updateBestSplit(split, mistakeCount)
    }

    bestStats
  }

  /** Helper to estimate cluster center bin from min+max (you can later cache this from clusterCenters) */
  private def estimateClusterCenterBins(stats: NodeFeatureStats): Map[Int, Int] = {
    stats.clusterBinCounts.map { case (clusterId, binMap) =>
      val total = binMap.values.sum
      val weightedAvgBin = binMap.map { case (bin, count) => bin * count }.sum / total
      val approxBin = math.round(weightedAvgBin).toInt
      clusterId -> approxBin
    }
  }

  /** Count mistakes based on cluster center bin location vs. actual instance bin location */
  private def computeMistakes(
      splitBin: Int,
      clusterBinCounts: Map[Int, Map[Int, Double]],
      centerBinLookup: Map[Int, Int]
  ): Long = {

    clusterBinCounts.map { case (clusterId, binCounts) =>
      val centerBin = centerBinLookup(clusterId)

      binCounts.map { case (bin, count) =>
        val isLeft = bin <= splitBin
        val centerIsLeft = centerBin <= splitBin

        if (isLeft != centerIsLeft) count else 0.0
      }.sum
    }.sum.toLong
  }
}
