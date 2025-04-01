package dimm.tree

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import dimm.core.{Instance, ClusterCenter}
import dimm.binning.FindSplits

class DecisionTreeBuilderTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("DecisionTreeBuilderTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("DecisionTreeBuilder builds tree with multiple clusters and instances") {
    val sc = spark.sparkContext

    // 3 clusters: roughly separated in feature space
    val instances = Seq(
      // Cluster 0
      Instance(0, 1.0, Vectors.dense(1.0)),
      Instance(0, 1.0, Vectors.dense(1.2)),
      Instance(0, 1.0, Vectors.dense(1.5)),

      // Cluster 1
      Instance(1, 1.0, Vectors.dense(4.5)),
      Instance(1, 1.0, Vectors.dense(4.8)),
      Instance(1, 1.0, Vectors.dense(5.1)),

      // Cluster 2
      Instance(2, 1.0, Vectors.dense(8.0)),
      Instance(2, 1.0, Vectors.dense(8.3)),
      Instance(2, 1.0, Vectors.dense(8.5))
    )

    val centers = Array(
      ClusterCenter(0, Vectors.dense(1.2)),
      ClusterCenter(1, Vectors.dense(4.8)),
      ClusterCenter(2, Vectors.dense(8.3))
    )

    val rdd = sc.parallelize(instances)

    val numFeatures = 1
    val numSplits = 4
    val maxBins = 32
    val numExamples = instances.length
    val weightedNumExamples = instances.map(_.weight).sum

    // Generate candidate splits
    val splits = FindSplits.findSplits(
      input = rdd,
      numFeatures = numFeatures,
      numSplits = numSplits,
      maxBins = maxBins,
      numExamples = numExamples,
      weightedNumExamples = weightedNumExamples,
      seed = 42L
    )

    val builder = new DecisionTreeBuilder(maxDepth = 3)
    val root = builder.buildTree(instances.toArray, centers, splits)

    println("\n===== Tree Structure =====")
    printTree(root)

    // Assertions
    assert(root.depth == 0)
    assert(root.left.nonEmpty && root.right.nonEmpty)
    assert(countNodes(root) <= math.pow(2, 4)) // no more than 2^depth nodes
    assert(countLeaves(root) > 0)
    assert(allLeavesHave1Or0Clusters(root))
  }

  // Helper: recursively print the tree
  def printTree(node: Node, indent: String = ""): Unit = {
    println(indent + node.toString)
    node.left.foreach(printTree(_, indent + "  "))
    node.right.foreach(printTree(_, indent + "  "))
  }

  // Helper: count total nodes
  def countNodes(node: Node): Int = {
    1 + node.left.map(countNodes).getOrElse(0) + node.right.map(countNodes).getOrElse(0)
  }

  // Helper: count total leaves
  def countLeaves(node: Node): Int = {
    if (node.isLeaf) 1
    else node.left.map(countLeaves).getOrElse(0) + node.right.map(countLeaves).getOrElse(0)
  }

  // Helper: check if leaves have <= 1 cluster
  def allLeavesHave1Or0Clusters(node: Node): Boolean = {
    if (node.isLeaf) node.numClusters <= 1
    else {
      node.left.forall(allLeavesHave1Or0Clusters) &&
      node.right.forall(allLeavesHave1Or0Clusters)
    }
  }
}
