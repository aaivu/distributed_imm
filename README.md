# Distributed IMM

A distributed implementation of Iterative Mistake Minimization (IMM) for clustering explanations. This package is designed to efficiently compute decision tree-based explanations for clustering tasks on large datasets using distributed systems like Apache Spark.

---

## Features

- Distributed computation of Iterative Mistake Minimization (IMM).
- Scalable implementation using PySpark for large datasets.
- Efficient histogram-based splitting using Cython.
- K-Means-based clustering for initialization.
- Customizable verbosity for debugging and performance tracking.
- Decision tree plotting with `graphviz`.

---

## Installation

### From PyPI (when hosted):
```bash
pip install distributed_imm