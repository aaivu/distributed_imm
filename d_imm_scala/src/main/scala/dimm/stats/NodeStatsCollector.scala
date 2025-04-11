package dimm.stats

import dimm.core.{BinnedInstance, BinnedCenter}
import dimm.tree.ContinuousSplit
import org.apache.spark.rdd.RDD

object NodeStatsCollector {

  def computeStats(
      instances: RDD[BinnedInstance],
      clusterCenters: RDD[BinnedCenter],
      splits: Array[Array[ContinuousSplit]]
  ): Map[(Int, Int), NodeFeatureStats] = {

    val validInstances = instances.filter(_.isValid)

    // Step 1: Count values per (nodeId, featureIndex, clusterId, bin)
    val binCounts = validInstances.flatMap { inst =>
      inst.binnedFeatures.zipWithIndex.map { case (bin, featureIndex) =>
        ((inst.nodeId, featureIndex, inst.clusterId, bin), inst.weight)
      }
    }.reduceByKey(_ + _)
     .map { case ((nodeId, featureIndex, clusterId, bin), weight) =>
       ((nodeId, featureIndex), Map(clusterId -> Map(bin -> weight)))
     }
     .reduceByKey { (m1, m2) =>
       mergeOuterMaps(m1, m2)
     }
     .collect()
     .toMap

    // Step 2: Min/max bin values per node-feature from cluster centers
    val clusterCenterBinMap = clusterCenters.flatMap { center =>
      center.binnedFeatures.zipWithIndex.map { case (bin, featureIndex) =>
        ((center.clusterId, featureIndex), bin)
      }
    }.groupByKey()
     .mapValues(bins => (bins.min, bins.max))
     .collect()
     .toMap

    // Step 3: Assemble NodeFeatureStats
    val stats = binCounts.map { case ((nodeId, featureIndex), clusterBinCounts) =>
      val allClusterIds = clusterBinCounts.keySet

      // Extract all relevant min/max from binned centers
      val clusterMinBins = allClusterIds.flatMap(cid => clusterCenterBinMap.get((cid, featureIndex)).map(_._1))
      val clusterMaxBins = allClusterIds.flatMap(cid => clusterCenterBinMap.get((cid, featureIndex)).map(_._2))

      val minBin = if (clusterMinBins.nonEmpty) clusterMinBins.min else 0
      val maxBin = if (clusterMaxBins.nonEmpty) clusterMaxBins.max else 0

      val stat = NodeFeatureStats(
        nodeId = nodeId,
        featureIndex = featureIndex,
        clusterBinCounts = clusterBinCounts,
        clusterMinBin = minBin,
        clusterMaxBin = maxBin
      )

      ((nodeId, featureIndex), stat)
    }

    stats
  }

  /** Merge outer maps like: Map[cid -> Map[bin -> count]] */
  private def mergeOuterMaps(
      m1: Map[Int, Map[Int, Double]],
      m2: Map[Int, Map[Int, Double]]
  ): Map[Int, Map[Int, Double]] = {
    (m1.keySet ++ m2.keySet).map { cid =>
      val binMap1 = m1.getOrElse(cid, Map.empty)
      val binMap2 = m2.getOrElse(cid, Map.empty)
      val mergedBinMap = (binMap1.keySet ++ binMap2.keySet).map { bin =>
        (bin, binMap1.getOrElse(bin, 0.0) + binMap2.getOrElse(bin, 0.0))
      }.toMap
      cid -> mergedBinMap
    }.toMap
  }
}
