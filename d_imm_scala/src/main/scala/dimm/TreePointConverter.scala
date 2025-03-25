package dimm

import org.apache.spark.rdd.RDD

object TreePointConverter {

  /**
   * Convert raw input data into binned TreePoint representation.
   */
  def convertToTreeRDD(
      input: RDD[Instance],
      splits: Array[Array[Split]],
      metadata: DecisionTreeMetadata): RDD[TreePoint] = {

    val thresholds: Array[Array[Double]] = splits.map { splitArr =>
      splitArr.map(_.asInstanceOf[ContinuousSplit].threshold)
    }

    input.map { instance =>
      val binnedFeatures = thresholds.indices.map { featureIndex =>
        val featureValue = instance.features(featureIndex)
        findBin(featureValue, thresholds(featureIndex))
      }.toArray
      TreePoint(instance.label, binnedFeatures, instance.weight)
    }
  }

  /**
   * Find which bin a value falls into based on sorted thresholds.
   */
  private def findBin(featureValue: Double, thresholds: Array[Double]): Int = {
    val idx = java.util.Arrays.binarySearch(thresholds, featureValue)
    if (idx >= 0) idx else -idx - 1
  }
}
