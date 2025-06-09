package dimm.stats

import dimm.tree.ContinuousSplit

object SplitEvaluator {

  /**
   * @param statsMap           Per-node, per-feature stats to evaluate splits on
   * @param centerBinLookup    Map from (clusterId, featureIndex) → binIndex, based on actual binned centers
   */
  def evaluate(
      statsMap: Map[(Int, Int), NodeFeatureStats],
      centerBinLookup: Map[(Int, Int), Int]
  ): Map[(Int, Int), NodeFeatureStats] = {
    statsMap.map { case (key @ (nodeId, featureIndex), stats) =>
      key -> findBestSplit(stats, featureIndex, centerBinLookup)
    }
  }

  def findBestSplit(
      stats: NodeFeatureStats,
      featureIndex: Int,
      centerBinLookup: Map[(Int, Int), Int]
  ): NodeFeatureStats = {
    if (!stats.isSplittable) return stats

    val minBin = stats.clusterMinBin
    val maxBin = stats.clusterMaxBin

    var bestStats = stats
    for (splitBin <- minBin to (maxBin - 1)) {
      val split = ContinuousSplit(featureIndex, threshold = splitBin.toDouble)

      val mistakeCount = computeMistakes(
        splitBin,
        stats.clusterBinCounts,
        featureIndex,
        centerBinLookup
      )

      bestStats = bestStats.updateBestSplit(split, mistakeCount)
    }

    bestStats
  }

  // def findBestSplit(
  //     stats: NodeFeatureStats,
  //     featureIndex: Int,
  //     centerBinLookup: Map[(Int, Int), Int]
  // ): NodeFeatureStats = {
  //   if (!stats.isSplittable) return stats

  //   val minBin = stats.clusterMinBin
  //   val maxBin = stats.clusterMaxBin

  //   // Use parallel collection for multiple candidate splits
  //   val best = (minBin to (maxBin - 1)).par
  //     .map { splitBin =>
  //       val split = ContinuousSplit(featureIndex, splitBin.toDouble)
  //       val mistakes = computeMistakes(splitBin, stats.clusterBinCounts, featureIndex, centerBinLookup)
  //       (split, mistakes)
  //     }
  //     .minBy(_._2)

  //   stats.updateBestSplit(best._1, best._2)
  // }


  /**
   * Compute IMM mistakes by comparing instance bin vs. cluster center bin
   */
  private def computeMistakes(
      splitBin: Int,
      clusterBinCounts: Map[Int, Map[Int, Double]],
      featureIndex: Int,
      centerBinLookup: Map[(Int, Int), Int]
  ): Long = {
    clusterBinCounts.map { case (clusterId, binCounts) =>
      val centerBin = centerBinLookup((clusterId, featureIndex))

      binCounts.map { case (bin, count) =>
        val isLeft = bin <= splitBin
        val centerIsLeft = centerBin <= splitBin

        if (isLeft != centerIsLeft) count else 0.0
      }.sum
    }.sum.toLong
  }
  // private def computeMistakes(
  //     splitBin: Int,
  //     clusterBinCounts: Map[Int, Map[Int, Double]],
  //     featureIndex: Int,
  //     centerBinLookup: Map[(Int, Int), Int]
  // ): Long = {
  //   var mistakes = 0.0
  //   for ((clusterId, binMap) <- clusterBinCounts) {
  //     val centerBin = centerBinLookup((clusterId, featureIndex))
  //     for ((bin, count) <- binMap) {
  //       val isLeft = bin <= splitBin
  //       val centerIsLeft = centerBin <= splitBin
  //       if (isLeft != centerIsLeft) mistakes += count
  //     }
  //   }
  //   mistakes.toLong
  // }

}
