package dimm

import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.mutable.OpenHashMap
// import org.apache.spark.ml.feature.Instance
import dimm.{Split, ContinuousSplit}


import org.apache.spark.ml.tree._

object SplitFinder {

  def findSplits(
      input: RDD[Instance],
      metadata: DecisionTreeMetadata,
      seed: Long): Array[Array[Split]] = {

    val continuousFeatures = (0 until metadata.numFeatures).filter(metadata.isContinuous)

    val sampledInput = {
      val fraction = samplesFractionForFindSplits(metadata)
      if (fraction < 1.0) {
        input.sample(withReplacement = false, fraction, new scala.util.Random(seed).nextInt()
)
      } else {
        input
      }
    }

    findSplitsBySorting(sampledInput, metadata, continuousFeatures)
  }


  def findSplitsBySorting(
      input: RDD[Instance],
      metadata: DecisionTreeMetadata,
      continuousFeatures: IndexedSeq[Int]): Array[Array[Split]] = {

    val numPartitions = math.min(continuousFeatures.length, input.partitions.length)

    val continuousSplits = input.flatMap { instance =>
      continuousFeatures.iterator.map { i =>
        (i, (instance.features(i), instance.weight))
      }.filter(_._2._1 != 0.0)
    }.aggregateByKey((new OpenHashMap[Double, Double], 0L), numPartitions)(
      seqOp = { case ((map, count), (v, w)) =>
        map.update(v, map.getOrElse(v, 0.0) + w)
        (map, count + 1L)
    }
    ,
      combOp = { case ((map1, c1), (map2, c2)) =>
        map2.foreach { case (v, w) =>
            map1.update(v, map1.getOrElse(v, 0.0) + w)
        }
        (map1, c1 + c2)
        }
    ).map { case (i,(valueMap, count)) =>
      val thresholds = findSplitsForContinuousFeature(valueMap.toMap, count, metadata)
      (i,thresholds.map(thresh => ContinuousSplit(i, thresh)))
    }.collectAsMap()

    Array.tabulate(metadata.numFeatures) { i =>
        if (metadata.isContinuous(i)) {
            val splits = continuousSplits.getOrElse(i, Array.empty[ContinuousSplit]).asInstanceOf[Array[Split]]
            metadata.setNumSplits(i, splits.length)
            splits
        } else {
            Array.empty[Split]
        }
    }
  }

  def findSplitsForContinuousFeature(
      valueWeights: Map[Double, Double],
      count: Long,
      metadata: DecisionTreeMetadata): Array[Double] = {

    val numSplits = metadata.numSplits
    val totalWeight = valueWeights.values.sum
    val weightedNumSamples = samplesFractionForFindSplits(metadata) * metadata.weightedNumExamples

    val tolerance = 1e-6 * count * 100
    val fullWeights = if ((weightedNumSamples - totalWeight).abs > tolerance) {
      valueWeights + (0.0 -> (weightedNumSamples - totalWeight))
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
      val stride = weightedNumSamples / (numSplits + 1)
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

  private def samplesFractionForFindSplits(metadata: DecisionTreeMetadata): Double = {
    val requiredSamples = math.max(metadata.maxBins * metadata.maxBins, 10000)
    if (requiredSamples < metadata.numExamples) {
      requiredSamples.toDouble / metadata.numExamples
    } else {
      1.0
    }
  }
}
