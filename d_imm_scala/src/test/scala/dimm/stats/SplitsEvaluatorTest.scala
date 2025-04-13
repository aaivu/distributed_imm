package dimm.stats

import org.scalatest.funsuite.AnyFunSuite
import dimm.tree.ContinuousSplit

class SplitEvaluatorTest extends AnyFunSuite {

  test("SplitEvaluator finds the correct split with minimal mistakes") {

    val stats = NodeFeatureStats(
      nodeId = 0,
      featureIndex = 0,
      clusterBinCounts = Map(
        // Cluster 0 center is near bin 1
        0 -> Map(0 -> 2.0, 1 -> 3.0, 2 -> 1.0),
        // Cluster 1 center is near bin 4
        1 -> Map(3 -> 2.0, 4 -> 4.0, 5 -> 2.0)
      ),
      clusterMinBin = 1,
      clusterMaxBin = 5
    )

    val updated = SplitEvaluator.findBestSplit(stats)

    // Check that we have a valid split
    assert(updated.bestSplit.isDefined, "No best split found")
    assert(updated.bestSplitMistakes >= 0)

    // Print the results for manual inspection
    println(s"Best split: ${updated.bestSplit.get}")
    println(s"Mistake count: ${updated.bestSplitMistakes}")

    // You may assert specific expectations depending on your heuristic
    val bestThreshold = updated.bestSplit.get.threshold.toInt

    // Based on center bins (estimation):
    // Cluster 0: centerBin ≈ 1
    // Cluster 1: centerBin ≈ 4
    // So best split should be around 2, 3, or 4 — separating clusters
    assert(bestThreshold >= 2 && bestThreshold <= 4)
  }
}
