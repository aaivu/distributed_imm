// package dimm.core

// import org.scalatest.funsuite.AnyFunSuite
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.rdd.RDD
// import dimm.core.{BinnedInstance, BinnedCenter}
// import dimm.tree.{Node, ContinuousSplit}
// import dimm.binning.FindSplits
// import dimm.core.IMMIteration

// class IMMIterationTest extends AnyFunSuite {

//   val spark = SparkSession.builder()
//     .appName("IMMIterationTest")
//     .master("local[*]")
//     .getOrCreate()

//   val sc = spark.sparkContext

//   test("IMMIteration performs one full iteration") {

//     // --- Setup synthetic data ---

//     // Instances (2 clusters, 2 features, in bins)
//     val instances: RDD[BinnedInstance] = sc.parallelize(Seq(
//       BinnedInstance(Array(1, 2), 0, 0, 1.0, true), // cluster 0 - left
//       BinnedInstance(Array(3, 2), 0, 0, 1.0, true), // cluster 0 - right (mistake)
//       BinnedInstance(Array(3, 2), 0, 1, 1.0, true), // cluster 1 - right
//     ))

//     // Cluster centers
//     val centers: RDD[BinnedCenter] = sc.parallelize(Seq(
//       BinnedCenter(Array(1, 2), 0),  // Cluster 0 → center bin = 1
//       BinnedCenter(Array(3, 2), 1)   // Cluster 1 → center bin = 3
//     ))

//     // Initial tree with one root node
//     val root = Node(id = 0, depth = 0, clusterIds = Set(0, 1), isLeaf = false)
//     val tree = Map(0 -> root)

//     // Feature splits (precomputed manually for test)
//     val splits: Array[Array[ContinuousSplit]] = Array(
//       Array(ContinuousSplit(0, 1.5), ContinuousSplit(0, 2.5)), // Feature 0
//       Array(ContinuousSplit(1, 1.5), ContinuousSplit(1, 2.5))  // Feature 1
//     )

//     // --- Run IMM Iteration ---
//     val (updatedInstances, updatedTree, _, converged) =
//       IMMIteration.runIteration(instances, centers, tree, splits, nodeIdCounter = 1)

//     // --- Assertions ---

//     val inst = updatedInstances.collect().sortBy(_.binnedFeatures(0))

//     println("Split applied at root. Children created:")
//     updatedTree.foreach { case (id, node) => println(s"Node $id: $node") }

//     val leftInstance = inst.find(_.binnedFeatures(0) == 1).get
//     assert(leftInstance.nodeId == 1)
//     assert(leftInstance.isValid)

//     val mistakeInstance = inst.find(i => i.binnedFeatures(0) == 3 && i.clusterId == 0).get
//     val mistakeNodeId = mistakeInstance.nodeId
//     println(s"mistakeInstance.nodeId: $mistakeNodeId")
//     assert(mistakeInstance.nodeId == 2)
//     assert(!mistakeInstance.isValid)

//     val rightInstance = inst.find(i => i.binnedFeatures(0) == 3 && i.clusterId == 1).get
//     assert(rightInstance.nodeId == 2)
//     assert(rightInstance.isValid)


//     // Check tree updated
//     assert(updatedTree.contains(1))
//     assert(updatedTree.contains(2))
//     assert(updatedTree(0).split.isDefined)

    

//     assert(!converged)
//   }
// }
