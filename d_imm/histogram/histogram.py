# -*- coding: utf-8 -*-
import math
import random
from collections import defaultdict, namedtuple
from pyspark.rdd import RDD
from pyspark.sql import SparkSession

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
        is_continuous: list,
        is_unordered: list,
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
        self.is_continuous = is_continuous
        self.is_unordered = is_unordered
        self.max_splits_per_feature = max_splits_per_feature
        self.max_bins = max_bins
        self.total_weighted_examples = total_weighted_examples
        self.seed = seed
        self.example_count = example_count

    def find_splits(self, input_rdd: RDD) -> list:
        """
        Public method to find splits for decision tree calculation.

        :param input_rdd: RDD of Instance objects.
        :return: A 2D list (list of lists) of Split objects [featureIndex -> list of Splits].
        """
        return self._find_splits_by_sorting(input_rdd)

    # @staticmethod
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

    def _find_splits_for_continuous_feature_values(self, feature_values) -> list:
        """
        Aggregates continuous feature values and counts, then computes split thresholds.

        :param feature_values: An iterable of numeric values (floats).
        :return: A list of Split objects representing split thresholds.
        """
        # Aggregate values into a dictionary: {feature_value -> total_count}
        value_counts = defaultdict(int)
        count = 0

        for v in feature_values:
            value_counts[v] += 1  # Each data point has weight = 1
            count += 1

        # Convert to a normal dict and call the helper function
        return self._find_splits_for_continuous_feature_weights(
            part_value_weights=dict(value_counts),
            count=count
        )

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

    def _find_splits_for_categorical_feature(
        self,
        categories: list,
        counts: list,
        is_unordered: bool
    ) -> list:
        """
        Computes split thresholds for a single categorical feature.

        :param categories: List of category names or indices.
        :param counts: List of counts corresponding to each category.
        :param is_unordered: Boolean indicating if the categorical feature is unordered.
        :return: A list of Split objects representing split thresholds.
        """
        num_splits = self.max_splits_per_feature[0]
        if is_unordered:
            # Handle unordered categorical features (multiclass with low arity)
            # Use one-vs-all splits for simplicity
            splits = []
            for cat in categories:
                splits.append(Split(
                    feature_index=-1,
                    threshold=None,
                    categories={cat},
                    is_continuous=False
                ))
                if len(splits) >= num_splits:
                    break
            return splits
        else:
            # Handle ordered categorical features
            # Sort categories and use stride-based splits
            sorted_categories_with_counts = sorted(zip(categories, counts), key=lambda x: x[0])
            sorted_categories = [x[0] for x in sorted_categories_with_counts]
            sorted_counts = [x[1] for x in sorted_categories_with_counts]

            # Number of possible splits is number of categories -1
            possible_splits = len(sorted_categories) - 1

            if possible_splits <= num_splits:
                # Return all possible splits by ordering
                splits = []
                for i in range(1, len(sorted_categories)):
                    left_cats = set(sorted_categories[:i])
                    splits.append(Split(
                        feature_index=-1,
                        threshold=None,
                        categories=left_cats,
                        is_continuous=False
                    ))
                return splits

            # Otherwise, use stride-based approach to distribute splits based on counts
            splits = []
            stride = sum(sorted_counts) / (num_splits + 1)
            current_sum = 0
            target = stride
            left_cats = set()

            for cat, cnt in zip(sorted_categories, sorted_counts):
                current_sum += cnt
                left_cats.add(cat)
                if current_sum >= target:
                    splits.append(Split(
                        feature_index=-1,
                        threshold=None,
                        categories=set(left_cats),
                        is_continuous=False
                    ))
                    target += stride
                    if len(splits) >= num_splits:
                        break

            return splits

    def _find_splits_by_sorting(self, sampled_input_rdd: RDD) -> list:
        """
        Finds split thresholds for both continuous and categorical features by sorting and aggregating.

        :param sampled_input_rdd: RDD of Instance objects.
        :return: A 2D list of Split objects. Outer list is indexed by feature, inner list contains splits for that feature.
        """
        # 1. Identify continuous and categorical features
        continuous_features = [i for i, cont in enumerate(self.is_continuous) if cont]
        categorical_features = [i for i, cont in enumerate(self.is_continuous) if not cont]

        # 2. Handle continuous features
        continuous_splits = self._find_splits_for_continuous_features(sampled_input_rdd, continuous_features)

        # 3. Handle categorical features
        categorical_splits = self._find_splits_for_categorical_features(sampled_input_rdd, categorical_features)

        # 4. Combine splits for all features
        all_splits = []
        for fidx in range(self.num_features):
            if self.is_continuous[fidx]:
                all_splits.append(continuous_splits.get(fidx, []))
            else:
                all_splits.append(categorical_splits.get(fidx, []))

        return all_splits

    def _find_splits_for_continuous_features(
        self,
        sampled_input_rdd: RDD,
        continuous_features: list
    ) -> dict:
        """
        Finds splits for continuous features.

        :param sampled_input_rdd: RDD of Instance objects.
        :param continuous_features: List of feature indices that are continuous.
        :return: Dictionary mapping feature index to list of Split objects.
        """
        if not continuous_features:
            return {}

        # For each Instance, emit (featureIndex, featureValue)
        feature_value_pairs = (
            sampled_input_rdd
            .flatMap(lambda inst: [
                (i, inst.features[i])
                for i in continuous_features
            ])
            .filter(lambda x: x[1] != 0.0)  # Optionally filter out zero values
        )

        # Aggregate counts for each feature and value
        feature_aggregates = (
            feature_value_pairs
            .map(lambda x: (x[0], x[1]))  # (featureIndex, featureValue)
            .map(lambda x: ((x[0], x[1]), 1))  # ((featureIndex, featureValue), 1)
            .reduceByKey(lambda a, b: a + b)  # ((featureIndex, featureValue), count)
            .map(lambda x: (x[0][0], (x[0][1], x[1])))  # (featureIndex, (featureValue, count))
        )

        # Collect as map: { featureIndex -> list of (featureValue, count) }
        feature_value_counts = feature_aggregates.groupByKey().mapValues(list).collectAsMap()

        # Now compute splits for each continuous feature
        continuous_splits = {}
        for fidx in continuous_features:
            value_weight_map = {v: c for v, c in feature_value_counts.get(fidx, [])}
            splits = self._find_splits_for_continuous_feature_weights(
                part_value_weights=value_weight_map,
                count=sum(value_weight_map.values())
            )
            # Assign the correct feature index to each split
            splits_with_index = [
                Split(
                    feature_index=fidx,
                    threshold=s.threshold,
                    categories=None,
                    is_continuous=True
                ) for s in splits
            ]
            continuous_splits[fidx] = splits_with_index

        return continuous_splits

    def _find_splits_for_categorical_features(
        self,
        sampled_input_rdd: RDD,
        categorical_features: list
    ) -> dict:
        """
        Finds splits for categorical features.

        :param sampled_input_rdd: RDD of Instance objects.
        :param categorical_features: List of feature indices that are categorical.
        :return: Dictionary mapping feature index to list of Split objects.
        """
        if not categorical_features:
            return {}

        # For each Instance, emit (featureIndex, category)
        feature_category_pairs = (
            sampled_input_rdd
            .flatMap(lambda inst: [
                (i, inst.features[i])
                for i in categorical_features
            ])
        )

        # Aggregate counts for each feature and category
        feature_aggregates = (
            feature_category_pairs
            .map(lambda x: (x[0], x[1]))  # (featureIndex, category)
            .map(lambda x: ((x[0], x[1]), 1))  # ((featureIndex, category), 1)
            .reduceByKey(lambda a, b: a + b)  # ((featureIndex, category), count)
            .map(lambda x: (x[0][0], (x[0][1], x[1])))  # (featureIndex, (category, count))
        )

        # Collect as map: { featureIndex -> list of (category, count) }
        feature_category_counts = feature_aggregates.groupByKey().mapValues(list).collectAsMap()

        # Now compute splits for each categorical feature
        categorical_splits = {}
        for fidx in categorical_features:
            category_count_pairs = feature_category_counts.get(fidx, [])
            categories = [x[0] for x in category_count_pairs]
            counts = [x[1] for x in category_count_pairs]
            unordered = self.is_unordered[fidx]

            splits = self._find_splits_for_categorical_feature(
                categories=categories,
                counts=counts,
                is_unordered=unordered
            )
            # Assign the correct feature index to each split
            splits_with_index = [
                Split(
                    feature_index=fidx,
                    threshold=s.threshold,
                    categories=s.categories,
                    is_continuous=False
                ) for s in splits
            ]
            categorical_splits[fidx] = splits_with_index

        return categorical_splits

    def _find_splits_for_categorical_feature(
        self,
        categories: list,
        counts: list,
        is_unordered: bool
    ) -> list:
        """
        Computes split thresholds for a single categorical feature.

        :param categories: List of category names or indices.
        :param counts: List of counts corresponding to each category.
        :param is_unordered: Boolean indicating if the categorical feature is unordered.
        :return: A list of Split objects representing split thresholds.
        """
        num_splits = self.max_splits_per_feature[0]
        if is_unordered:
            # Handle unordered categorical features (multiclass with low arity)
            # Use one-vs-all splits for simplicity
            splits = []
            for cat in categories:
                splits.append(Split(
                    feature_index=-1,
                    threshold=None,
                    categories={cat},
                    is_continuous=False
                ))
                if len(splits) >= num_splits:
                    break
            return splits
        else:
            # Handle ordered categorical features
            # Sort categories and use stride-based splits
            sorted_categories_with_counts = sorted(zip(categories, counts), key=lambda x: x[0])
            sorted_categories = [x[0] for x in sorted_categories_with_counts]
            sorted_counts = [x[1] for x in sorted_categories_with_counts]

            # Number of possible splits is number of categories -1
            possible_splits = len(sorted_categories) - 1

            if possible_splits <= num_splits:
                # Return all possible splits by ordering
                splits = []
                for i in range(1, len(sorted_categories)):
                    left_cats = set(sorted_categories[:i])
                    splits.append(Split(
                        feature_index=-1,
                        threshold=None,
                        categories=left_cats,
                        is_continuous=False
                    ))
                return splits

            # Otherwise, use stride-based approach to distribute splits based on counts
            splits = []
            stride = sum(sorted_counts) / (num_splits + 1)
            current_sum = 0
            target = stride
            left_cats = set()

            for cat, cnt in zip(sorted_categories, sorted_counts):
                current_sum += cnt
                left_cats.add(cat)
                if current_sum >= target:
                    splits.append(Split(
                        feature_index=-1,
                        threshold=None,
                        categories=set(left_cats),
                        is_continuous=False
                    ))
                    target += stride
                    if len(splits) >= num_splits:
                        break

            return splits
