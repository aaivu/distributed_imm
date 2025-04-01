package dimm.core

import org.apache.spark.ml.linalg.Vector

case class Instance(
  clusterId: Int,           // Cluster label assigned by KMeans
  weight: Double,           // Optional weight, default is 1.0
  features: Vector,         // Feature vector
  isValid: Boolean = true   // Mark false if it's a mistake after a split
)
