package dimm.tree

trait Split {
  def featureIndex: Int
  def threshold: Double

  def goLeft(featureValue: Double): Boolean = {
    featureValue <= threshold
  }
}

case class ContinuousSplit(
  featureIndex: Int,
  threshold: Double
) extends Split {
  override def toString: String = s"(f$featureIndex <= $threshold)"
}
