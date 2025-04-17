package dimm.core

import dimm.binning.{FindSplits, BinAndAssign}
import dimm.tree.{Node}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import dimm.tree.ContinuousSplit

object IMMRunner {

  def runIMM(
      clusteredInstances: RDD[Instance],
      clusterCenters: Array[Vector],
      numSplits: Int = 10,
      maxBins: Int = 32,
      seed: Long = 42L
  ): (Map[Int, Node], Array[Array[ContinuousSplit]]) = {

    val numFeatures = clusterCenters.head.size
    val numExamples = clusteredInstances.count().toInt
    val weightedNumExamples = clusteredInstances.map(_.weight).sum().toDouble

    val splits = FindSplits.findSplits(
      input = clusteredInstances,
      clusterCenters = clusterCenters,
      numFeatures = numFeatures,
      numSplits = numSplits,
      maxBins = maxBins,
      numExamples = numExamples,
      weightedNumExamples = weightedNumExamples,
      seed = seed
    )

    val binnedInstances = BinAndAssign.binInstancesRDD(clusteredInstances, splits).cache()
    val binnedCenters = BinAndAssign.binClusterCentersRDD(clusterCenters, splits).cache()

    val allClusters = clusteredInstances.map(_.clusterId).distinct().collect().toSet
    var tree = Map(0 -> Node(id = 0, depth = 0, clusterIds = allClusters))
    var currentInstances = binnedInstances
    var nodeCounter = 1
    var done = false

    while (!done) {
      val (nextInstances, nextTree, nextCounter, converged) =
        IMMIteration.runIteration(currentInstances, binnedCenters, tree, splits, nodeCounter)

      currentInstances = nextInstances
      tree = nextTree
      nodeCounter = nextCounter
      done = converged
    }

    (tree, splits)
  }
}
