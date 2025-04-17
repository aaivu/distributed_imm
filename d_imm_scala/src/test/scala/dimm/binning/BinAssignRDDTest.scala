package dimm.binning

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import dimm.core.{Instance, BinnedInstance, BinnedCenter}
import dimm.tree.ContinuousSplit
// import dimm.binning.BinAndAssign

class BinAndAssignRDDTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("BinAndAssignRDDTest")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  test("binInstancesRDD and binClusterCentersRDD bin values as expected") {

    val instances: RDD[Instance] = sc.parallelize(Seq(
      Instance(clusterId = 0, weight = 1.0, features = Vectors.dense(1.0, 2.5)),
      Instance(clusterId = 1, weight = 1.0, features = Vectors.dense(4.0, 3.7))
    ))

    val clusterCenters = Array(
      Vectors.dense(3.0, 3.5),
      Vectors.dense(5.0, 4.5)
    )

    val splits: Array[Array[ContinuousSplit]] = Array(
      Array(ContinuousSplit(0, 2.0), ContinuousSplit(0, 4.0)), // Feature 0 splits
      Array(ContinuousSplit(1, 3.0), ContinuousSplit(1, 4.0))  // Feature 1 splits
    )

    val binnedInstances: Array[BinnedInstance] = BinAndAssign
      .binInstancesRDD(instances, splits)
      .collect()

    val binnedCenters: Array[BinnedCenter] = BinAndAssign
      .binClusterCentersRDD(clusterCenters, splits)
      .collect()

    // Feature 0 splits: 2.0, 4.0 → bins: <=2.0 → 0, <=4.0 → 1, >4.0 → 2
    // Feature 1 splits: 3.0, 4.0 → bins: <=3.0 → 0, <=4.0 → 1, >4.0 → 2

    // Instance 1: (1.0, 2.5) → bin 0, 0
    // Instance 2: (4.0, 3.7) → bin 1, 1

    assert(binnedInstances(0).binnedFeatures.sameElements(Array(0, 0)))
    assert(binnedInstances(1).binnedFeatures.sameElements(Array(1, 1)))

    // Center 1: (3.0, 3.5) → bin 1, 1
    // Center 2: (5.0, 4.5) → bin 2, 2

    assert(binnedCenters(0).binnedFeatures.sameElements(Array(1, 1)))
    assert(binnedCenters(1).binnedFeatures.sameElements(Array(2, 2)))
  }
}
