package dimm.core

import dimm.tree.ContinuousSplit
import org.apache.spark.ml.linalg.Vector

object MistakeCounter {

  case class SplitResult(
    leftPoints: Array[Instance],
    rightPoints: Array[Instance],
    leftCenters: Array[ClusterCenter],
    rightCenters: Array[ClusterCenter],
    leftMistakes: Int,
    rightMistakes: Int
  )

  def countMistakes(
      instances: Array[Instance],
      centers: Array[ClusterCenter],
      split: ContinuousSplit
  ): SplitResult = {

    val featureIndex = split.featureIndex
    val threshold = split.threshold

    val (leftCenters, rightCenters) = centers.partition { c =>
      c.features(featureIndex) <= threshold
    }

    val centerMapLeft = leftCenters.map(c => c.clusterId -> c).toMap
    val centerMapRight = rightCenters.map(c => c.clusterId -> c).toMap

    val (leftPoints, rightPoints, leftMistakes, rightMistakes) =
      instances.foldLeft((List.empty[Instance], List.empty[Instance], 0, 0)) {
        case ((lPoints, rPoints, lMistakes, rMistakes), point) =>
          val featureValue = point.features(featureIndex)
          val goesLeft = featureValue <= threshold
          val clusterId = point.clusterId

          if (goesLeft) {
            val isMistake = !centerMapLeft.contains(clusterId)
            val updated = point.copy(isValid = !isMistake)
            (
              updated :: lPoints,
              rPoints,
              lMistakes + (if (isMistake) 1 else 0),
              rMistakes
            )
          } else {
            val isMistake = !centerMapRight.contains(clusterId)
            val updated = point.copy(isValid = !isMistake)
            (
              lPoints,
              updated :: rPoints,
              lMistakes,
              rMistakes + (if (isMistake) 1 else 0)
            )
          }
      }

    SplitResult(
      leftPoints = leftPoints.toArray,
      rightPoints = rightPoints.toArray,
      leftCenters = leftCenters,
      rightCenters = rightCenters,
      leftMistakes = leftMistakes,
      rightMistakes = rightMistakes
    )
  }
}
