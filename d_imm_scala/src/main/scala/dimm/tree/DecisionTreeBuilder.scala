private def buildNode(
    instances: Array[Instance],
    centers: Array[ClusterCenter],
    depth: Int,
    splits: Array[Array[ContinuousSplit]]
): Node = {
  val validPoints = instances.filter(_.isValid)
  val node = Node(
    id = nextNodeId(),
    depth = depth,
    points = validPoints,
    centers = centers
  )

  if (depth >= maxDepth || node.shouldStopSplitting || validPoints.isEmpty) {
    return node.markAsLeaf()
  }

  var bestSplit: Option[ContinuousSplit] = None
  var bestResult: Option[SplitResult] = None
  var minMistakes: Int = Int.MaxValue

  for (featureIndex <- splits.indices) {
    // ---- 1. Extract valid thresholds from global splits
    val originalSplits = splits(featureIndex).map(_.threshold)

    // ---- 2. Add cluster center values for this feature
    val centerFeatureValues = centers.map(_.features(featureIndex))

    // ---- 3. Combine & filter within center value range
    val minCenterVal = centerFeatureValues.min
    val maxCenterVal = centerFeatureValues.max

    val combinedThresholds = (originalSplits ++ centerFeatureValues)
      .filter(t => t >= minCenterVal && t <= maxCenterVal)
      .distinct
    //   .sorted

    // ---- 4. Count mistakes for each candidate threshold
    for (threshold <- combinedThresholds) {
      val split = ContinuousSplit(featureIndex, threshold)
      val result = MistakeCounter.countMistakes(validPoints, centers, split)
      val totalMistakes = result.leftMistakes + result.rightMistakes

      if (totalMistakes < minMistakes) {
        minMistakes = totalMistakes
        bestSplit = Some(split)
        bestResult = Some(result)
      }
    }
  }

  if (bestSplit.isEmpty || bestResult.isEmpty) {
    return node.markAsLeaf()
  }

  val result = bestResult.get

  val leftChild = buildNode(result.leftPoints, result.leftCenters, depth + 1, splits)
  val rightChild = buildNode(result.rightPoints, result.rightCenters, depth + 1, splits)

  node.withSplit(bestSplit.get, leftChild, rightChild, minMistakes)
}
