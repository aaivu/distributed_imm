package dimm.tree

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import dimm.core.BinnedInstance
import dimm.stats.BestSplitDecision

class TreeBuilderTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("TreeBuilderTest")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  test("TreeBuilder correctly updates tree with children and clusters") {

    // Initial root node
    val root = Node(
      id = 0,
      depth = 0,
      clusterIds = Set(0, 1),
      isLeaf = false
    )

    val treeMap = Map(0 -> root)

    val bestSplits = Map(
      0 -> BestSplitDecision(
        nodeId = 0,
        featureIndex = 0,
        split = ContinuousSplit(0, 2.0),
        mistakeCount = 1
      )
    )

    val updatedInstances: RDD[BinnedInstance] = sc.parallelize(Seq(
      BinnedInstance(Array(1, 1), nodeId = 1, clusterId = 0, weight = 1.0, isValid = true), // left child
      BinnedInstance(Array(3, 2), nodeId = 2, clusterId = 1, weight = 1.0, isValid = true), // right child
      BinnedInstance(Array(3, 2), nodeId = 2, clusterId = 1, weight = 1.0, isValid = true)
    ))

    val updatedTree = TreeBuilder.updateTree(treeMap, bestSplits, updatedInstances)

    // Check parent node
    val parent = updatedTree(0)
    assert(parent.split.isDefined)
    assert(parent.leftChild.isDefined && parent.rightChild.isDefined)

    // Check left child
    val left = updatedTree(1)
    assert(left.clusterIds == Set(0))
    assert(left.isLeaf)

    // Check right child
    val right = updatedTree(2)
    assert(right.clusterIds == Set(1))
    assert(right.isLeaf)

    println("Updated Tree:")
    updatedTree.toSeq.sortBy(_._1).foreach { case (id, node) =>
      println(s"Node $id: $node")
    }
  }
}
