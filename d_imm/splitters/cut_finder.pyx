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
def get_all_mistakes_histogram(
    NP_FLOAT_t[:,:] X, NP_INT_t[:] y, NP_FLOAT_t[:,:] centers,
    NP_INT_t[:] valid_centers, NP_INT_t[:] valid_cols, list histogram, int njobs
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
                update_col_all_mistakes_histogram_unsorted(
                    X, y, centers, valid_centers,
                    col, n, k, feature_results, histogram[col]
                )
    else:
        # Parallelize the process
        for col in prange(d, num_threads=njobs, schedule='dynamic', nogil=True):  # Removed nogil=True
            if valid_cols[col] == 1:
                with gil:
                    update_col_all_mistakes_histogram_unsorted(
                        X, y, centers, valid_centers,
                        col, n, k, feature_results, histogram[col]
                    )
    
    return feature_results


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
            else:
                if centers[y[ix], col] <= threshold:
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