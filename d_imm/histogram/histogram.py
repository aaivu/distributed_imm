# -*- coding: utf-8 -*-
from collections import defaultdict, namedtuple
from pyspark.rdd import RDD

# Define a small epsilon for floating-point comparisons
EPSILON = 1e-9

# Define the Instance and Split namedtuples
Instance = namedtuple("Instance", ["features", "label", "weight"])
Split = namedtuple("Split", ["feature_index", "threshold", "categories", "is_continuous"])


class DecisionTreeSplitFinder:
    """
    A class to compute split thresholds for decision tree training,
    handling both continuous and categorical features.
    """

    def __init__(
        self,
        num_features: int,
        max_splits_per_feature: list,
        max_bins: int,
        total_weighted_examples: float,
        seed: int = 42,
        example_count: int = 10000
    ):
        """
        Initializes the DecisionTreeSplitFinder with the necessary parameters.

        :param num_features: Total number of features.
        :param is_continuous: List indicating if each feature is continuous.
        :param is_unordered: List indicating if each categorical feature is unordered.
                             Must align with categorical feature indices.
        :param max_splits_per_feature: List specifying the maximum number of splits allowed per feature.
        :param max_bins: Parameter used for binning or quantile calculations.
        :param total_weighted_examples: Total weighted number of examples in the dataset.
        :param seed: Random seed for reproducibility during sampling.
        """
        self.num_features = num_features
        self.max_splits_per_feature = max_splits_per_feature
        self.max_bins = max_bins
        self.total_weighted_examples = total_weighted_examples
        self.seed = seed
        self.example_count = example_count

    def find_splits(self, sampled_input_rdd: RDD) -> list:
        """
        Computes split thresholds for continuous features by sorting and aggregating.

        :param sampled_input_rdd: RDD of Instance objects.
        :return: A 2D list of Split objects, where the outer list is indexed by feature, 
                 and the inner list contains splits for that feature.
        """

        # Extract feature-value pairs for all instances
        feature_value_pairs = (
            sampled_input_rdd
            .flatMap(lambda inst: [(i, inst.features[i]) for i in range(self.num_features)])
            .filter(lambda x: x[1] != 0.0)  # Optionally filter out zero values
        )

        # Aggregate counts for each feature-value pair
        feature_aggregates = (
            feature_value_pairs
            .map(lambda x: ((x[0], x[1]), 1))  # ((featureIndex, featureValue), 1)
            .reduceByKey(lambda a, b: a + b)  # ((featureIndex, featureValue), count)
            .map(lambda x: (x[0][0], (x[0][1], x[1])))  # (featureIndex, (featureValue, count))
        )

        # Collect as a dictionary: { featureIndex -> list of (featureValue, count) }
        feature_value_counts = feature_aggregates.groupByKey().mapValues(list).collectAsMap()

        # Compute splits for each feature
        all_splits = []
        for fidx in range(self.num_features):
            value_weight_map = {v: c for v, c in feature_value_counts.get(fidx, [])}
            splits = self._find_splits_for_continuous_feature_weights(
                part_value_weights=value_weight_map,
                count=sum(value_weight_map.values())
            )
            # Assign feature index to each split
            all_splits.append([
                Split(
                    feature_index=fidx,
                    threshold=s.threshold,
                    categories=None,
                    is_continuous=True
                ) for s in splits
            ])

        return all_splits

    def _find_splits_for_continuous_feature_weights(
        self,
        part_value_weights: dict,
        count: int
    ) -> list:
        """
        Computes split thresholds for a single continuous feature.

        :param part_value_weights: Dict of { feature_value -> count }.
        :param count: Total number of data points aggregated for this feature.
        :return: A list of Split objects representing split thresholds.
        """
        # If no values exist, return empty.
        if not part_value_weights:
            return []

        # Sum of counts for this feature.
        part_num_samples = sum(part_value_weights.values())

        # Compute the fraction of the data to use for splits
        fraction = self._samples_fraction_for_find_splits(
            max_bins=self.max_bins,
            num_examples=count
        )

        # Weighted number of samples (since weights are counts)
        weighted_num_samples = fraction * float(count)

        # Tolerance for floating-point adjustments
        tolerance = EPSILON * count * 100

        # Add zero-value count if needed
        # If the expected number of samples minus the actual is greater than tolerance, add a zero count
        if weighted_num_samples - part_num_samples > tolerance:
            part_value_weights = dict(part_value_weights)  # Make a copy to avoid mutating the original
            additional_count = weighted_num_samples - part_num_samples
            part_value_weights[0.0] = part_value_weights.get(0.0, 0.0) + additional_count

        # Sort the values
        sorted_pairs = sorted(part_value_weights.items(), key=lambda x: x[0])  # [(value, count), ...]

        # Number of possible splits is number of intervals between sorted values
        possible_splits = len(sorted_pairs) - 1

        if possible_splits == 0:
            # All feature values are the same => no splits
            return []

        if possible_splits <= self.max_splits_per_feature[0]:
            # If we have fewer or equal intervals compared to allowed splits, return all midpoints
            splits = []
            for i in range(1, len(sorted_pairs)):
                left_val = sorted_pairs[i - 1][0]
                right_val = sorted_pairs[i][0]
                midpoint = (left_val + right_val) / 2.0
                splits.append(Split(
                    feature_index=-1,
                    threshold=midpoint,
                    categories=None,
                    is_continuous=True
                ))  # feature_index to be set later
            return splits

        # Otherwise, use stride-based approach
        stride = weighted_num_samples / (self.max_splits_per_feature[0] + 1)

        splits_builder = []
        index = 1
        current_count = sorted_pairs[0][1]
        target_count = stride

        while index < len(sorted_pairs):
            previous_count = current_count
            current_count += sorted_pairs[index][1]
            previous_gap = abs(previous_count - target_count)
            current_gap = abs(current_count - target_count)

            if previous_gap < current_gap:
                # Place a split threshold between previous value and current value
                left_val = sorted_pairs[index - 1][0]
                right_val = sorted_pairs[index][0]
                midpoint = (left_val + right_val) / 2.0
                splits_builder.append(Split(
                    feature_index=-1,
                    threshold=midpoint,
                    categories=None,
                    is_continuous=True
                ))
                target_count += stride

            index += 1

        return splits_builder

    def _samples_fraction_for_find_splits(self, max_bins: int, num_examples: int) -> float:
        """
        Calculate the subsample fraction for finding splits based on max_bins and num_examples.

        :param max_bins: Maximum number of bins used for splitting.
        :param num_examples: Number of examples (rows) in the dataset.
        :return: A float representing the fraction of data to use.
        """
        required_samples = max(max_bins * max_bins, self.example_count)
        if required_samples < num_examples:
            return float(required_samples) / num_examples
        else:
            return 1.0