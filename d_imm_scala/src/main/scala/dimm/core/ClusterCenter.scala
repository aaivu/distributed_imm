package dimm.core

import org.apache.spark.ml.linalg.Vector

case class ClusterCenter(
  clusterId: Int,
  features: Vector
)