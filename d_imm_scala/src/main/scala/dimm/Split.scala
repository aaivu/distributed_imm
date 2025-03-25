package dimm

trait Split extends Serializable {
  def shouldGoLeft(featureValue: Double): Boolean
  def featureIndex: Int
}

case class ContinuousSplit(featureIndex: Int, threshold: Double) extends Split {
  override def shouldGoLeft(featureValue: Double): Boolean = {
    featureValue <= threshold
  }

  override def toString: String = s"(feature $featureIndex <= $threshold)"
}
