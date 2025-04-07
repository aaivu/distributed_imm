package dimm.tree

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.rdd.RDD
import dimm.core.{Instance, ClusterCenter}
import dimm.binning.FindSplits

class IrisDecisionTreeBuilderTest extends AnyFunSuite {

  val spark = SparkSession.builder()
    .appName("IrisDecisionTreeBuilderTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("DecisionTreeBuilder works on Iris dataset with cluster labels") {
    val irisRDD = loadIrisAsInstances("test_data/iris_stacked.csv")

    val clusterCenters = Array(
      ClusterCenter(0, Vectors.dense(5.0, 3.4, 1.5, 0.2)),
      ClusterCenter(1, Vectors.dense(6.0, 2.8, 4.5, 1.5)),
      ClusterCenter(2, Vectors.dense(6.5, 3.0, 5.5, 2.0))
    )

    val numFeatures = 4
    val numSplits = 10
    val maxBins = 32
    val numExamples = irisRDD.count().toInt
    val weightedNumExamples = irisRDD.map(_.weight).sum()

    val splits = FindSplits.findSplits(
      input = irisRDD,
      numFeatures = numFeatures,
      numSplits = numSplits,
      maxBins = maxBins,
      numExamples = numExamples,
      weightedNumExamples = weightedNumExamples,
      seed = 1234L
    )

    println("\n===== Candidate Splits =====")
    splits.zipWithIndex.foreach { case (splitArr, featureIdx) =>
      println(s"Feature $featureIdx splits:")
      splitArr.foreach(split => println(s"  $split"))
    }

    val instances = irisRDD.collect()
    val builder = new DecisionTreeBuilder(maxDepth = 4)
    val root = builder.buildTree(instances, clusterCenters, splits)

    println("\n===== Tree Structure =====")
    printTree(root)

    assert(countLeaves(root) > 0)
    assert(allLeavesHave1Or0Clusters(root))
  }

  def loadIrisAsInstances(path: String): RDD[Instance] = {
    val df = spark.read.option("header", "true").csv(path)

    df.rdd.zipWithIndex.map { case (row, idx) =>
      val features = Vectors.dense(
        row.getString(0).toDouble,
        row.getString(1).toDouble,
        row.getString(2).toDouble,
        row.getString(3).toDouble
      )

      // Use row index to assign fake cluster (0, 1, 2)
      val clusterId = (idx % 3).toInt

      Instance(
        clusterId = clusterId,
        weight = 1.0,
        features = features
      )
    }
  }

  def printTree(node: Node, indent: String = ""): Unit = {
    println(indent + node.toString)
    node.left.foreach(printTree(_, indent + "  "))
    node.right.foreach(printTree(_, indent + "  "))
  }

  def countLeaves(node: Node): Int = {
    if (node.isLeaf) 1
    else node.left.map(countLeaves).getOrElse(0) + node.right.map(countLeaves).getOrElse(0)
  }

  def allLeavesHave1Or0Clusters(node: Node): Boolean = {
    if (node.isLeaf) node.numClusters <= 1
    else {
      node.left.forall(allLeavesHave1Or0Clusters) &&
      node.right.forall(allLeavesHave1Or0Clusters)
    }
  }
}
