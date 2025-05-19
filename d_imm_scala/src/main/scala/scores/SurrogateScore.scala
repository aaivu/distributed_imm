package dimm.score

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object SurrogateKMeansCost {

  /**
   * Computes surrogate K-means cost using IMM tree-assigned clusters,
   * but distances are to original Spark KMeans centers.
   *
   * @param assigned RDD[(clusterId, feature vector)] from IMM tree
   * @param originalCenters original cluster centers from KMeans model
   * @return surrogate cost
   */
  def compute(assigned: RDD[(Int, Vector)], originalCenters: Array[Vector]): Double = {
    assigned.map { case (clusterId, features) =>
      Vectors.sqdist(features, originalCenters(clusterId))
    }.sum()
  }
}
