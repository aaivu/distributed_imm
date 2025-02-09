# distutils: language = c
# cython: boundscheck = False
# cython: wraparound = False
# cython: profile = False

import numpy as np
cimport numpy as np
cimport cython
from cython.parallel import prange
from libc.stdlib cimport malloc, free

ctypedef np.int32_t NP_INT_t
ctypedef np.float64_t NP_FLOAT_t

cdef extern from "<math.h>" nogil:
    const float INFINITY

cdef extern from "<limits.h>":
    const int INT_MIN
    const int INT_MAX

@cython.boundscheck(False)
@cython.wraparound(False)
def get_all_mistakes(
    NP_FLOAT_t[:,:] X, NP_INT_t[:] y, NP_FLOAT_t[:,:] centers,
    NP_INT_t[:] valid_centers, NP_INT_t[:] valid_cols, int njobs
):
    """
    Main function to calculate mistakes for all features and thresholds.
    Returns a list of Python dictionaries with 'feature', 'threshold', and 'mistakes'.
    """
    cdef int n = X.shape[0]
    cdef int k = centers.shape[0]
    cdef int d = valid_cols.shape[0]
    cdef int *centers_count = <int *> malloc(k * sizeof(int))
    cdef list feature_results = []
    cdef int col

    # Initialize center counts
    for i in range(k):
        centers_count[i] = 0
    for i in range(n):
        centers_count[y[i]] += 1

    if njobs is None or njobs <= 1:
        # Iterate over valid columns
        for col in range(d):
            if valid_cols[col] == 1:
                update_col_all_mistakes(
                    X, y, centers, valid_centers, centers_count,
                    col, n, k, feature_results
                )
    else:
        # Parallelize the process
        for col in prange(d, nogil=True, num_threads=njobs):
            if valid_cols[col] == 1:
                update_col_all_mistakes(
                    X, y, centers, valid_centers, centers_count,
                    col, n, k, feature_results
                )

    free(centers_count)
    return feature_results


@cython.boundscheck(False)
@cython.wraparound(False)
cdef void update_col_all_mistakes(
    NP_FLOAT_t[:,:] X, NP_INT_t[:] y, NP_FLOAT_t[:,:] centers,
    NP_INT_t[:] valid_centers, int* centers_count,
    int col, int n, int k,
    list feature_results) nogil:
    """
    Helper function to calculate mistakes for a single column.
    Appends the results as Python dictionaries to `feature_results`.
    """
    cdef int i
    cdef int ix
    cdef int ic
    cdef int mistakes
    cdef NP_FLOAT_t prev_threshold
    cdef NP_FLOAT_t threshold
    cdef NP_FLOAT_t max_val
    cdef np.int64_t[:] data_order
    cdef np.int64_t[:] centers_order
    cdef int *left_centers_count = <int *> malloc(k * sizeof(int))
    cdef int curr_center_idx
    cdef bint is_center_threshold

    # Sort data points and centers
    with gil:
        data_order = np.asarray(X[:, col]).argsort()
        centers_order = np.asarray(centers[:, col]).argsort()

    # Find the maximum valid center value
    max_val = -INFINITY
    for i in range(k):
        if valid_centers[i] == 1:
            if centers[i, col] > max_val:
                max_val = centers[i, col]

    # Initialize mistakes and left_centers_count
    for i in range(k):
        left_centers_count[i] = 0

    ix = 0
    ic = 0
    mistakes = 0

    # Advance center index to the first valid one
    while ic < k and valid_centers[centers_order[ic]] == 0:
        ic += 1

    # Handle the case where there are no valid centers
    if ic >= k:
        free(left_centers_count)
        return

    # The first threshold
    threshold = centers[centers_order[ic], col]
    is_center_threshold = 1

    # Initialize mistakes and left-centers-counts for the first threshold
    while ix < n and X[data_order[ix], col] <= threshold:
        curr_center_idx = y[data_order[ix]]  # Center of the current data point
        left_centers_count[curr_center_idx] += 1
        if centers[curr_center_idx, col] >= threshold:
            mistakes += 1
        ix += 1

    # Store the result for this threshold
    with gil:
        feature_results.append({
            'feature': col,
            'threshold': threshold,
            'mistakes': mistakes
        })

    # Main loop to iterate over all thresholds
    while ix < n - 1 or ic < k:
        prev_threshold = threshold

        if threshold >= max_val:
            break

        # Process current threshold
        if is_center_threshold == 0:  # Threshold is a data point
            curr_center_idx = y[data_order[ix]]
            left_centers_count[curr_center_idx] += 1

            if centers[curr_center_idx, col] >= threshold:
                mistakes += 1
            elif centers[curr_center_idx, col] < threshold:
                mistakes -= 1

            ix += 1
        else:  # Threshold is a center
            mistakes += centers_count[centers_order[ic]] - 2 * left_centers_count[centers_order[ic]]
            ic += 1

            # Safeguard against infinite loop for invalid centers
            while ic < k and valid_centers[centers_order[ic]] == 0:
                ic += 1

        # Update the next threshold
        if ix < n and ic < k:
            if X[data_order[ix], col] <= centers[centers_order[ic], col]:
                threshold = X[data_order[ix], col]
                is_center_threshold = 0
            else:
                threshold = centers[centers_order[ic], col]
                is_center_threshold = 1
        elif ix < n:
            threshold = X[data_order[ix], col]
            is_center_threshold = 0
        elif ic < k:
            threshold = centers[centers_order[ic], col]
            is_center_threshold = 1
        else:
            break

        # Store the result for this threshold
        if prev_threshold != threshold:
            with gil:
                feature_results.append({
                    'feature': col,
                    'threshold': prev_threshold,
                    'mistakes': mistakes
                })

    free(left_centers_count)

@cython.boundscheck(False)
@cython.wraparound(False)
def get_all_mistakes_histogram(
    NP_FLOAT_t[:,:] X, NP_INT_t[:] y, NP_FLOAT_t[:,:] centers,
    NP_INT_t[:] valid_centers, NP_INT_t[:] valid_cols, list histogram, int njobs, bint sorted=True
):
    """
    Main function to calculate mistakes for all features and thresholds.
    Returns a list of Python dictionaries with 'feature', 'threshold', and 'mistakes'.
    """
    cdef int n = X.shape[0]
    cdef int k = centers.shape[0]
    cdef int d = valid_cols.shape[0]
    cdef list feature_results = []
    cdef int col

    if njobs is None or njobs <= 1:
        # Iterate over valid columns
        for col in range(d):
            if valid_cols[col] == 1:
                if sorted:
                    update_col_all_mistakes_histogram_sorted(
                        X, y, centers, valid_centers,
                        col, n, k, feature_results, histogram[col]
                    )
                else:
                    update_col_all_mistakes_histogram_unsorted(
                        X, y, centers, valid_centers,
                        col, n, k, feature_results, histogram[col]
                    )
    else:
        # Parallelize the process
        # for col in prange(d, num_threads=njobs):  # Removed nogil=True
        #     with gil:
        #         if valid_cols[col] == 1:
        #             if sorted:
        #                 update_col_all_mistakes_histogram_sorted(
        #                     X, y, centers, valid_centers,
        #                     col, n, k, feature_results, histogram[col]
        #                 )
        #             else:
        #                 update_col_all_mistakes_histogram_unsorted(
        #                     X, y, centers, valid_centers,
        #                     col, n, k, feature_results, histogram[col]
        #                 )
        pass

    return feature_results


@cython.boundscheck(False)
@cython.wraparound(False)
cdef void update_col_all_mistakes_histogram_sorted(
    NP_FLOAT_t[:,:] X, NP_INT_t[:] y, NP_FLOAT_t[:,:] centers,
    NP_INT_t[:] valid_centers,
    int col, int n, int k,
    list feature_results, list histogram) nogil:  # Marked as noexcept
    """
    Helper function to calculate mistakes for a single column using histogram-based and center-based thresholds.
    Appends the results as Python dictionaries to feature_results.
    """
    cdef int i
    cdef int ix = 0
    cdef int ic = 0
    cdef int mistakes = 0
    cdef NP_FLOAT_t threshold
    cdef np.int64_t[:] data_order
    cdef np.int64_t[:] centers_order
    cdef int num_thresholds
    cdef NP_FLOAT_t *combined_thresholds
    cdef NP_FLOAT_t max_val
    cdef NP_FLOAT_t min_val

    # Find the maximum and minimum valid center values
    max_val = -INFINITY
    min_val = INFINITY

    for i in range(k):
        if valid_centers[i] == 1:
            if centers[i, col] > max_val:
                max_val = centers[i, col]
            if centers[i, col] < min_val:
                min_val = centers[i, col]

    # Sorting and threshold combination
    with gil:
        data_order = np.asarray(X[:, col]).argsort()
        centers_order = np.asarray(centers[:, col]).argsort()

        # Combine histogram-based and center-based thresholds
        unique_thresholds = set()
        for split in histogram:
            if min_val <= split.threshold < max_val:
                unique_thresholds.add(split.threshold)

        for i in range(k):
            if valid_centers[i] == 1 and centers[i, col]!=max_val:
                unique_thresholds.add(centers[i, col])

        sorted_thresholds = sorted(unique_thresholds)

        num_thresholds = len(sorted_thresholds)
        combined_thresholds = <NP_FLOAT_t *> malloc(num_thresholds * sizeof(NP_FLOAT_t))

        for i in range(num_thresholds):
            combined_thresholds[i] = sorted_thresholds[i]

    # Process each threshold
    for i in range(num_thresholds):
        mistakes = 0
        ix = 0
        threshold = combined_thresholds[i]

        # Count mistakes
        while ix < n:
            if X[data_order[ix], col] <= threshold:
                if centers[y[data_order[ix]], col] > threshold:
                    mistakes += 1
            else:
                break
            ix += 1

        # Store result
        with gil:
            feature_results.append({
                'feature': col,
                'threshold': threshold,
                'mistakes': mistakes
            })

    free(combined_thresholds)


@cython.boundscheck(False)
@cython.wraparound(False)
cdef void update_col_all_mistakes_histogram_unsorted(
    NP_FLOAT_t[:,:] X, NP_INT_t[:] y, NP_FLOAT_t[:,:] centers,
    NP_INT_t[:] valid_centers,
    int col, int n, int k,
    list feature_results, list histogram) nogil:  # Marked as noexcept
    """
    Helper function to calculate mistakes for a single column using histogram-based and center-based thresholds.
    Appends the results as Python dictionaries to feature_results.
    """
    cdef int i
    cdef int ix = 0
    cdef int ic = 0
    cdef int mistakes = 0
    cdef NP_FLOAT_t threshold
    cdef np.int64_t[:] centers_order
    cdef int num_thresholds
    cdef NP_FLOAT_t *combined_thresholds
    cdef NP_FLOAT_t max_val
    cdef NP_FLOAT_t min_val

    # Find the maximum and minimum valid center values
    max_val = -INFINITY
    min_val = INFINITY

    for i in range(k):
        if valid_centers[i] == 1:
            if centers[i, col] > max_val:
                max_val = centers[i, col]
            if centers[i, col] < min_val:
                min_val = centers[i, col]

    # Sorting and threshold combination
    with gil:
        centers_order = np.asarray(centers[:, col]).argsort()

        # Combine histogram-based and center-based thresholds
        unique_thresholds = set()
        for split in histogram:
            if min_val <= split.threshold < max_val:
                unique_thresholds.add(split.threshold)

        for i in range(k):
            if valid_centers[i] == 1 and centers[i, col]!=max_val:
                unique_thresholds.add(centers[i, col])

        sorted_thresholds = sorted(unique_thresholds)

        num_thresholds = len(sorted_thresholds)
        combined_thresholds = <NP_FLOAT_t *> malloc(num_thresholds * sizeof(NP_FLOAT_t))

        for i in range(num_thresholds):
            combined_thresholds[i] = sorted_thresholds[i]

    # Process each threshold
    for i in range(num_thresholds):
        mistakes = 0
        ix = 0
        threshold = combined_thresholds[i]

        # Count mistakes
        while ix < n:
            if X[ix, col] <= threshold:
                if centers[y[ix], col] > threshold:
                    mistakes += 1
            ix += 1

        # Store result
        with gil:
            feature_results.append({
                'feature': col,
                'threshold': threshold,
                'mistakes': mistakes
            })

    free(combined_thresholds)