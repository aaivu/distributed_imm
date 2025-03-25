package dimm

import org.apache.spark.ml.linalg.Vector

private[dimm] case class Instance(label: Double, weight: Double, features: Vector)
