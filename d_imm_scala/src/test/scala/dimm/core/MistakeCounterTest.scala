package dimm.core

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.ml.linalg.Vectors
import dimm.tree.ContinuousSplit

class MistakeCounterTest extends AnyFunSuite {

  test("MistakeCounter counts mistakes correctly") {
    // 2 cluster centers
    val centers = Array(
      ClusterCenter(clusterId = 0, features = Vectors.dense(2.0)),
      ClusterCenter(clusterId = 1, features = Vectors.dense(6.0))
    )

    // 4 data points assigned to those clusters
    val instances = Array(
      Instance(clusterId = 0, weight = 1.0, features = Vectors.dense(1.5)), // left, correct
      Instance(clusterId = 1, weight = 1.0, features = Vectors.dense(3.0)), // right, mistake
      Instance(clusterId = 0, weight = 1.0, features = Vectors.dense(5.5)), // left, mistake
      Instance(clusterId = 1, weight = 1.0, features = Vectors.dense(6.5))  // right, correct
    )

    // Apply a split at 5.0 on feature index 0
    val split = ContinuousSplit(featureIndex = 0, threshold = 5.0)

    val result = MistakeCounter.countMistakes(instances, centers, split)

    println(s"Left mistakes: ${result.leftMistakes}, Right mistakes: ${result.rightMistakes}")

    assert(result.leftMistakes == 1)
    assert(result.rightMistakes == 1)

    val leftClusterIds = result.leftCenters.map(_.clusterId).toSet
    val rightClusterIds = result.rightCenters.map(_.clusterId).toSet

    assert(leftClusterIds == Set(0))
    assert(rightClusterIds == Set(1))

    assert(result.leftPoints.exists(p => !p.isValid))  // at least one invalid in left
    assert(result.rightPoints.exists(p => !p.isValid)) // at least one invalid in right
  }
}
