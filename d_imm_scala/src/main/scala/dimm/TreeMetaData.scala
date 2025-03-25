package dimm

class DecisionTreeMetadata(
    val numFeatures: Int,
    val numSplits: Int,
    val weightedNumExamples: Double,
    val maxBins: Int,
    val numExamples: Long,
    val isContinuous: Int => Boolean
) extends Serializable {

  private val _numSplitsPerFeature = Array.fill(numFeatures)(numSplits)

  def setNumSplits(featureIndex: Int, value: Int): Unit = {
    _numSplitsPerFeature(featureIndex) = value
  }

  def getNumSplits(featureIndex: Int): Int = _numSplitsPerFeature(featureIndex)

  def numSplitsArray: Array[Int] = _numSplitsPerFeature.clone()
}
