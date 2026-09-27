# D-IMM: Distributed Iterative Mistake Minimization

D-IMM is a distributed implementation of the **Iterative Mistake Minimization (IMM)** algorithm ([Dasgupta et al., 2020](https://arxiv.org/abs/2002.12538)) built on **Apache Spark**. It produces global, post-hoc explanations for k-means clustering by building a small decision tree whose leaves approximate the k-means cluster assignments, and it scales to datasets that do not fit in a single machine's memory.

## How It Works

1. **Clustering** – Run k-means (Spark MLlib) on the input data.
2. **Split discovery** – Executors compute per-feature histograms; the driver derives candidate thresholds from quantiles plus the k cluster-center values, bounded to the range of the centers.
3. **Binning** – Instances and cluster centers are discretized once and cached, so later split checks are integer comparisons.
4. **Tree initialization** – A root node covering all clusters is created.
5. **Iteration loop** – In parallel, collect node–feature statistics, compute mistake counts for each candidate threshold, pick the lowest-mistake split per tree node, reassign points, and create child/leaf nodes. Repeat until all leaves are pure.

## Requirements

| Component | Version |
|-----------|---------|
| Scala     | 2.12.18 |
| Apache Spark | 3.5.4 |
| Java      | 17 |

## Build

```bash
sbt clean package
```

## Usage

```bash
spark-submit \
  --class <MainClass> \
  --master <master-url> \
  --num-executors <N> \
  target/scala-2.12/<d-imm-jar>.jar \
  <input-path> <k> [num-bins]
```

Replace the placeholders with the values for your build and cluster.

### Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `k` | Number of k-means clusters | — |
| `num-bins` | Minimum quantile bins per feature (the k center values are added as extra thresholds, giving up to `bins + k` split points) | 32 |

Tree induction stops when every leaf contains a single cluster.

## Datasets

Experiments in the paper used two datasets from the UCI ML Repository:

- **HIGGS** – 11M instances, 28 features ([DOI](https://doi.org/10.24432/C5V312))
- **SUSY** – 5M instances, 18 features ([DOI](https://doi.org/10.24432/C54606))

The datasets are not included in this repository.

## Results (Summary)

On Google Cloud Dataproc (n4-standard-4 master, 2/4/8 n4-standard-2 workers):

- Up to **3.29× speedup** over single-node IMM on HIGGS with 8 executors.
- Mistake percentages and surrogate costs comparable to single-node IMM on HIGGS.
- On small workloads (e.g. SUSY, 1M instances) Spark's coordination overhead can make D-IMM slower than single-node IMM; the benefit grows with data size and k.

See the paper for full tables and figures.

## Authors

Inuka Ampavila, Bojitha Liyanage, Salim Marium, Uthayasanker Thayasivam (University of Moratuwa), Sumanaruban Rajadurai (OCTAVE – John Keells Holdings PLC)

## Citation

If you use this work, please cite:

```
I. Ampavila, B. Liyanage, S. Marium, U. Thayasivam and S. Rajadurai, "D-IMM: Distributed Iterative Mistake Minimization," 2026 Moratuwa Engineering Research Conference (MERCon), Moratuwa, Sri Lanka, 2026, pp. 706-711, doi: 10.1109/MERCon71835.2026.11691525. keywords: {Scalability;Runtime;Machine learning;Costing;Costs;Minimization;Modeling;Trees (botanical);Vegetation;Explainable AI;Explainable AI;distributed clustering;Apache Spark;k-means;Iterative Mistake Minimization},
```

## References

- S. Dasgupta, N. Frost, M. Moshkovitz, C. Rashtchian, "Explainable k-means and k-medians clustering," 2020.
- N. Frost, M. Moshkovitz, C. Rashtchian, "ExKMC: Expanding explainable k-means clustering," 2020.
  
## License
MIT License



