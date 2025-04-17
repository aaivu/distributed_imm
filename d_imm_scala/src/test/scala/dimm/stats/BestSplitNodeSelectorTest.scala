package dimm.stats

import org.scalatest.funsuite.AnyFunSuite
import dimm.tree.ContinuousSplit

class BestSplitPerNodeSelectorTest extends AnyFunSuite {

  test("BestSplitPerNodeSelector picks the best split per node") {

    val statsMap: Map[(Int, Int), NodeFeatureStats] = Map(

      // Node 0, Feature 0
      (0, 0) -> NodeFeatureStats(
        nodeId = 0,
        featureIndex = 0,
        clusterBinCounts = Map.empty,
        clusterMinBin = 1,
        clusterMaxBin = 5,
        bestSplit = Some(ContinuousSplit(0, 2.0)),
        bestSplitMistakes = 5
      ),

      // Node 0, Feature 1 (better)
      (0, 1) -> NodeFeatureStats(
        nodeId = 0,
        featureIndex = 1,
        clusterBinCounts = Map.empty,
        clusterMinBin = 1,
        clusterMaxBin = 5,
        bestSplit = Some(ContinuousSplit(1, 3.0)),
        bestSplitMistakes = 2
      ),

      // Node 1, Feature 0
      (1, 0) -> NodeFeatureStats(
        nodeId = 1,
        featureIndex = 0,
        clusterBinCounts = Map.empty,
        clusterMinBin = 1,
        clusterMaxBin = 4,
        bestSplit = Some(ContinuousSplit(0, 2.0)),
        bestSplitMistakes = 7
      ),

      // Node 1, Feature 1 (better)
      (1, 1) -> NodeFeatureStats(
        nodeId = 1,
        featureIndex = 1,
        clusterBinCounts = Map.empty,
        clusterMinBin = 1,
        clusterMaxBin = 4,
        bestSplit = Some(ContinuousSplit(1, 2.0)),
        bestSplitMistakes = 3
      )
    )

    val bestPerNode = BestSplitPerNodeSelector.selectBestPerNode(statsMap)

    // Node 0 should select feature 1 with 2 mistakes
    assert(bestPerNode.contains(0))
    assert(bestPerNode(0).featureIndex == 1)
    assert(bestPerNode(0).mistakeCount == 2)

    // Node 1 should select feature 1 with 3 mistakes
    assert(bestPerNode.contains(1))
    assert(bestPerNode(1).featureIndex == 1)
    assert(bestPerNode(1).mistakeCount == 3)

    println("Best splits per node:")
    bestPerNode.foreach { case (nid, split) =>
      println(s"Node $nid => $split")
    }
  }
}
