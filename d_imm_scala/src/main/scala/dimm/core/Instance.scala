package dimm.core

import org.apache.spark.ml.linalg.Vector

case class Instance(
  clusterId: Int,           // Cluster label assigned by KMeans
  weight: Double,           // Optional weight, default is 1.0
  features: Vector,         // Feature vector
  isValid: Boolean = true   // Mark false if it's a mistake after a split
)

case class BinnedInstance(
  binnedFeatures: Array[Int],  // Index of the bin for each feature
  nodeId: Int,                 // Current node ID (starts with root = 0)
  clusterId: Int,              // Original cluster assigned by KMeans
  weight: Double,              // Instance weight
  isValid: Boolean             // Whether this instance is valid in the current iteration
)