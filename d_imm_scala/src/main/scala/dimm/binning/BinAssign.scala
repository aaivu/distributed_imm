package dimm.binning

import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.Vector
import dimm.core.{Instance, BinnedInstance, BinnedCenter}
import dimm.tree.ContinuousSplit

object BinAndAssign {

  def binInstance(
      instance: Instance,
      splits: Array[Array[ContinuousSplit]]
  ): BinnedInstance = {
    val numFeatures = instance.features.size
    val binnedFeatures = Array.ofDim[Int](numFeatures)

    for (i <- 0 until numFeatures) {
      val value = instance.features(i)
      val featureSplits = splits(i).map(_.threshold)
      binnedFeatures(i) = findBin(value, featureSplits)
    }

    BinnedInstance(
      binnedFeatures = binnedFeatures,
      nodeId = 0,
      clusterId = instance.clusterId,
      weight = instance.weight,
      isValid = true
    )
  }

  def binClusterCenter(
      clusterId: Int,
      vector: Vector,
      splits: Array[Array[ContinuousSplit]]
  ): BinnedCenter = {
    val numFeatures = vector.size
    val binnedFeatures = Array.ofDim[Int](numFeatures)

    for (i <- 0 until numFeatures) {
      val value = vector(i)
      val featureSplits = splits(i).map(_.threshold)
      binnedFeatures(i) = findBin(value, featureSplits)
    }

    BinnedCenter(
      binnedFeatures = binnedFeatures,
      clusterId = clusterId
    )
  }

  // 🆕 RDD versions
  def binInstancesRDD(
      input: RDD[Instance],
      splits: Array[Array[ContinuousSplit]]
  ): RDD[BinnedInstance] = {
    val broadcastSplits = input.context.broadcast(splits)
    input.map(instance => binInstance(instance, broadcastSplits.value))
  }

  def binClusterCentersRDD(
      centers: Array[Vector],
      splits: Array[Array[ContinuousSplit]]
  ): RDD[BinnedCenter] = {
    val sparkContext = org.apache.spark.SparkContext.getOrCreate()
    val centerWithIds = centers.zipWithIndex.map { case (vec, idx) => (idx, vec) }
    val rdd = sparkContext.parallelize(centerWithIds)
    val broadcastSplits = sparkContext.broadcast(splits)

    rdd.map { case (id, vec) =>
      binClusterCenter(id, vec, broadcastSplits.value)
    }
  }

  private def findBin(value: Double, thresholds: Array[Double]): Int = {
    var left = 0
    var right = thresholds.length - 1
    while (left <= right) {
      val mid = (left + right) / 2
      if (value <= thresholds(mid)) right = mid - 1
      else left = mid + 1
    }
    left
  }
}
