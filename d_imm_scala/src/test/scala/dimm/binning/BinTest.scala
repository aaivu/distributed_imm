package dimm.binning

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.ml.linalg.Vectors
import dimm.core.{Instance, BinnedInstance, BinnedCenter}
import dimm.tree.ContinuousSplit
// import dimm.binning.BinAndAssignTest

class BinAndAssignTest extends AnyFunSuite {

  test("Bin data point and cluster center using consistent split logic") {
    val splits: Array[Array[ContinuousSplit]] = Array(
      Array(ContinuousSplit(0, 1.5), ContinuousSplit(0, 3.0), ContinuousSplit(0, 5.0)),
      Array(ContinuousSplit(1, 2.0), ContinuousSplit(1, 3.5))
    )

    val instance = Instance(
      clusterId = 1,
      weight = 1.0,
      features = Vectors.dense(2.5, 3.0)
    )

    val clusterCenter = Vectors.dense(5.0, 3.5)

    val binnedInstance: BinnedInstance = BinAndAssign.binInstance(instance, splits)
    val binnedCenter: BinnedCenter = BinAndAssign.binClusterCenter(1, clusterCenter, splits)

    // Check that values fall into expected bins
    // Feature 0 split at 1.5, 3.0, 5.0 => 2.5 -> bin 1, 5.0 -> bin 3
    // Feature 1 split at 2.0, 3.5 => 3.0 -> bin 1, 3.5 -> bin 1 (since goLeft is <=)
    assert(binnedInstance.binnedFeatures.sameElements(Array(1, 1)))
    assert(binnedCenter.binnedFeatures.sameElements(Array(2, 1)))
    assert(binnedInstance.nodeId == 0)
    assert(binnedInstance.isValid)
  }
}
