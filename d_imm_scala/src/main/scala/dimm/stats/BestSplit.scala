package dimm.stats

import dimm.tree.ContinuousSplit

case class BestSplitDecision(
  nodeId: Int,
  featureIndex: Int,
  split: ContinuousSplit,
  mistakeCount: Long
)
