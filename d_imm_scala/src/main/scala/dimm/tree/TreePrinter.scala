package dimm.tree

object TreePrinter {

  def printTree(tree: Map[Int, Node], splits: Array[Array[ContinuousSplit]]): Unit = {
    println("\n=== Final IMM Explanation Tree ===")
    tree.toSeq.sortBy(_._1).foreach { case (id, node) =>
      println(s"Node $id: ${formatNode(node, splits)}")
    }
  }

  private def formatNode(node: Node, splits: Array[Array[ContinuousSplit]]): String = {
    val label = if (node.isLeaf) "Leaf" else s"Node ${node.id}"
    val splitStr = node.split.map(s => getRealThresholdString(s, splits)).getOrElse("No split")
    val clusterStr = node.clusterIds.mkString(",")
    val sampleStr = node.samples.map(s => s"samples=$s").getOrElse("samples=?")
    val mistakeStr = node.mistakes.map(m => s"mistakes=$m").getOrElse("mistakes=?")

    s"$label | depth=${node.depth} | split=$splitStr | clusters=[$clusterStr] | $sampleStr | $mistakeStr"
  }

  private def getRealThresholdString(split: ContinuousSplit, splits: Array[Array[ContinuousSplit]]): String = {
    val binIndex = split.threshold.toInt
    if (split.featureIndex < splits.length && binIndex < splits(split.featureIndex).length) {
      val realThreshold = splits(split.featureIndex)(binIndex).threshold
      f"(f${split.featureIndex} <= $realThreshold%.3f)"
    } else {
      s"(f${split.featureIndex} <= [bin ${split.threshold}])"
    }
  }
}
