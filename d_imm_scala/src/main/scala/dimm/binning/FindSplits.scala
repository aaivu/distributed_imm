package dimm.binning

import dimm.core.Instance
import dimm.tree.ContinuousSplit
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import scala.collection.mutable

object FindSplits {

  def findSplits(
      input: RDD[Instance],
      clusterCenters: Array[Vector],
      numFeatures: Int,
      numSplits: Int,
      maxBins: Int,
      numExamples: Int,
      weightedNumExamples: Double,
      seed: Long
  ): Array[Array[ContinuousSplit]] = {

    val continuousFeatures = 0 until numFeatures

    val fraction = samplesFractionForFindSplits(maxBins, numExamples)
    val sampledInput = if (fraction < 1.0) {
      input.sample(withReplacement = false, fraction, seed)
    } else {
      input
    }

    val numPartitions = math.min(numFeatures, sampledInput.partitions.length)

    val featureValueWeights = sampledInput.flatMap { instance =>
      continuousFeatures.map { featureIndex =>
        (featureIndex, (instance.features(featureIndex), instance.weight))
      }.filter(_._2._1 != 0.0)
    }.aggregateByKey((mutable.Map.empty[Double, Double], 0L), numPartitions)(
      seqOp = { case ((map, count), (v, w)) =>
        map.update(v, map.getOrElse(v, 0.0) + w)
        (map, count + 1L)
      },
      combOp = { case ((map1, c1), (map2, c2)) =>
        map2.foreach { case (v, w) =>
          map1.update(v, map1.getOrElse(v, 0.0) + w)
        }
        (map1, c1 + c2)
      }
    ).collectAsMap()

    // Collect cluster center values per feature
    val centerValuesByFeature: Array[mutable.Set[Double]] = Array.fill(numFeatures)(mutable.Set.empty[Double])
    clusterCenters.foreach { center =>
      for (f <- 0 until numFeatures) {
        val value = center(f)
        if (value != 0.0) centerValuesByFeature(f).add(value)
      }
    }

    val splitsByFeature: Array[Array[ContinuousSplit]] = Array.tabulate(numFeatures) { featureIndex =>
      val centerSplits = centerValuesByFeature(featureIndex)

      featureValueWeights.get(featureIndex) match {
        case Some((valueMap: scala.collection.Map[Double, Double], count: Long)) =>
          val dataSplits = findThresholdsForFeature(valueMap.toMap, count, numSplits, maxBins, weightedNumExamples)
          val allSplits = dedup((dataSplits ++ centerSplits).toArray, centerSplits.toSet)
          allSplits.map(t => ContinuousSplit(featureIndex, t))
        case _ =>
          val allSplits = dedup(centerSplits.toArray, centerSplits.toSet)
          allSplits.map(t => ContinuousSplit(featureIndex, t))
      }
    }

    splitsByFeature
  }

  private def findThresholdsForFeature(
      valueWeights: Map[Double, Double],
      count: Long,
      numSplits: Int,
      maxBins: Int,
      weightedNumExamples: Double
  ): Array[Double] = {

    val requiredSamples = math.max(maxBins * maxBins, 10000)
    val tolerance = 1e-6 * count * 100
    val totalWeight = valueWeights.values.sum

    val fullWeights = if ((weightedNumExamples - totalWeight).abs > tolerance) {
      valueWeights + (0.0 -> (weightedNumExamples - totalWeight))
    } else {
      valueWeights
    }

    val sorted = fullWeights.toSeq.sortBy(_._1).toArray
    val possibleSplits = sorted.length - 1

    if (possibleSplits == 0) {
      Array.empty[Double]
    } else if (possibleSplits <= numSplits) {
      (1 to possibleSplits).map(i => (sorted(i - 1)._1 + sorted(i)._1) / 2.0).toArray
    } else {
      val stride = weightedNumExamples / (numSplits + 1)
      val splits = mutable.ArrayBuilder.make[Double]
      var index = 1
      var currentWeight = sorted(0)._2
      var target = stride

      while (index < sorted.length) {
        val prevWeight = currentWeight
        currentWeight += sorted(index)._2
        if ((prevWeight - target).abs < (currentWeight - target).abs) {
          splits += (sorted(index - 1)._1 + sorted(index)._1) / 2.0
          target += stride
        }
        index += 1
      }

      splits.result()
    }
  }

  private def samplesFractionForFindSplits(maxBins: Int, numExamples: Int): Double = {
    val requiredSamples = math.max(maxBins * maxBins, 10000)
    if (requiredSamples < numExamples) {
      requiredSamples.toDouble / numExamples
    } else {
      1.0
    }
  }

  private def dedup(
      thresholds: Array[Double],
      centerValues: Set[Double],
      tol: Double = 1e-9
  ): Array[Double] = {
    thresholds.sorted.foldLeft(Vector.empty[Double]) { (acc, v) =>
      val tooClose = acc.exists(prev => math.abs(prev - v) <= tol)
      if (!tooClose || centerValues.contains(v)) acc :+ v else acc
    }.toArray
  }
}
