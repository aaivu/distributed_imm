package dimm.eval

import dimm.core.Instance
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object SurrogateEvaluator {

  /** Calculates the true KMeans cost using recomputed means per tree cluster */
  def computeKMeansCost(
      instances: RDD[(Instance, Int)]  // (instance, treeClusterId)
  ): Double = {
    val grouped = instances.map { case (inst, clusterId) =>
      (clusterId, (inst.features, 1))
    }

    val clusterStats = grouped
      .reduceByKey { case ((v1, c1), (v2, c2)) =>
        (sumVectors(v1, v2), c1 + c2)
      }
      .mapValues { case (sumVec, count) =>
        scaleVector(sumVec, 1.0 / count)
      }
      .collectAsMap()

    val bcCenters = instances.context.broadcast(clusterStats)

    instances.map { case (inst, clusterId) =>
      val center = bcCenters.value(clusterId)
      squaredDistance(inst.features, center)
    }.sum()
  }

  /** Calculates the surrogate cost using the original KMeans centers */
  def computeSurrogateCost(
      instances: RDD[(Instance, Int)],  // (instance, treeClusterId)
      originalCenters: Array[Vector]    // KMeans centers
  ): Double = {
    val bcOriginal = instances.context.broadcast(originalCenters)

    instances.map { case (inst, clusterId) =>
      val center = bcOriginal.value(inst.clusterId)
      squaredDistance(inst.features, center)
    }.sum()
  }

  private def squaredDistance(v1: Vector, v2: Vector): Double = {
    require(v1.size == v2.size)
    var sum = 0.0
    var i = 0
    while (i < v1.size) {
      val d = v1(i) - v2(i)
      sum += d * d
      i += 1
    }
    sum
  }

  private def sumVectors(v1: Vector, v2: Vector): Vector = {
    Vectors.dense(v1.toArray.zip(v2.toArray).map { case (a, b) => a + b })
  }

  private def scaleVector(v: Vector, scalar: Double): Vector = {
    Vectors.dense(v.toArray.map(_ * scalar))
  }
}
