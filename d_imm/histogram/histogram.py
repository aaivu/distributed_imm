from collections import defaultdict, namedtuple
from pyspark.rdd import RDD
import numpy as np
import heapq

EPSILON = 1e-9

Instance = namedtuple("Instance", ["features", "label", "weight"])
Split = namedtuple("Split", ["feature_index", "threshold", "categories", "is_continuous"])

class DecisionTreeSplitFinder:
    def __init__(self, num_features, max_splits_per_feature, max_bins, total_weighted_examples, seed=42, example_count=10000):
        self.num_features = num_features
        self.max_splits_per_feature = max_splits_per_feature
        self.max_bins = max_bins
        self.total_weighted_examples = total_weighted_examples
        self.seed = seed
        self.example_count = example_count

    def find_splits(self, sampled_input_rdd: RDD) -> list:
        sampled_input_rdd = sampled_input_rdd.persist()

        feature_value_pairs = sampled_input_rdd.flatMap(lambda inst: [(i, inst.features[i]) for i in range(self.num_features)])
        
        feature_aggregates = feature_value_pairs.map(lambda x: ((x[0], x[1]), 1)).combineByKey(
            lambda v: v, lambda c, v: c + v, lambda c1, c2: c1 + c2
        ).map(lambda x: (x[0][0], (x[0][1], x[1])))

        feature_value_counts = dict(feature_aggregates.groupByKey().mapValues(list).toLocalIterator())

        all_splits = []
        for fidx in range(self.num_features):
            value_weight_map = {v: c for v, c in feature_value_counts.get(fidx, [])}
            splits = self._find_splits_for_continuous_feature_weights(value_weight_map, sum(value_weight_map.values()))
            all_splits.append([
                Split(feature_index=fidx, threshold=s.threshold, categories=None, is_continuous=True) for s in splits
            ])

        return all_splits

    def _find_splits_for_continuous_feature_weights(self, part_value_weights: dict, count: int) -> list:
        if not part_value_weights:
            return []

        total_weight = sum(part_value_weights.values())
        fraction = self._samples_fraction_for_find_splits(self.max_bins, count)
        weighted_num_samples = fraction * float(count)

        if weighted_num_samples - total_weight > EPSILON * count * 100:
            part_value_weights = dict(part_value_weights)
            additional_count = weighted_num_samples - total_weight
            part_value_weights[0.0] = part_value_weights.get(0.0, 0.0) + additional_count

        sorted_pairs = heapq.nsmallest(len(part_value_weights), part_value_weights.items(), key=lambda x: x[0])

        values = np.array([v for v, _ in sorted_pairs])
        counts = np.array([c for _, c in sorted_pairs])
        cumulative_counts = np.cumsum(counts)

        if len(values) - 1 <= self.max_splits_per_feature[0]:
            return [Split(feature_index=-1, threshold=(values[i - 1] + values[i]) / 2, categories=None, is_continuous=True) 
                    for i in range(1, len(values))]

        stride = weighted_num_samples / (self.max_splits_per_feature[0] + 1)
        split_positions = np.linspace(cumulative_counts[0], cumulative_counts[-1], self.max_splits_per_feature[0] + 2)[1:-1]
        split_indices = np.searchsorted(cumulative_counts, split_positions)
        split_thresholds = (values[split_indices - 1] + values[split_indices]) / 2.0

        return [Split(feature_index=-1, threshold=t, categories=None, is_continuous=True) for t in split_thresholds]

    def _samples_fraction_for_find_splits(self, max_bins: int, num_examples: int) -> float:
        required_samples = max(max_bins * max_bins, self.example_count)
        return min(1.0, float(required_samples) / num_examples)
