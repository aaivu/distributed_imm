// package dimm.eval

// import dimm.core.Instance
// import org.apache.spark.ml.linalg.Vectors
// import org.apache.spark.sql.SparkSession
// import org.scalatest.funsuite.AnyFunSuite

// class SurrogateEvaluatorTest extends AnyFunSuite {

//   val spark = SparkSession.builder()
//     .appName("SurrogateEvaluatorTest")
//     .master("local[*]")
//     .getOrCreate()

//   import spark.implicits._

//   test("compute KMeans cost and surrogate cost with 2 clusters") {
//     val sc = spark.sparkContext

//     // Create mock data
//     val instances = sc.parallelize(Seq(
//       (Instance(0, 1.0, Vectors.dense(1.0, 1.0)), 0),
//       (Instance(0, 1.0, Vectors.dense(2.0, 2.0)), 0),
//       (Instance(1, 1.0, Vectors.dense(10.0, 10.0)), 1),
//       (Instance(1, 1.0, Vectors.dense(11.0, 11.0)), 1)
//     ))

//     // Original KMeans centers for cluster 0 and 1
//     val centers = Array(
//       Vectors.dense(1.5, 1.5),     // Original center for cluster 0
//       Vectors.dense(10.5, 10.5)    // Original center for cluster 1
//     )

//     // Run both metrics
//     val kmeansCost = SurrogateEvaluator.computeKMeansCost(instances)
//     val surrogateCost = SurrogateEvaluator.computeSurrogateCost(instances, centers)

//     println(f"True KMeans Cost: $kmeansCost%.3f")
//     println(f"Surrogate Cost:   $surrogateCost%.3f")

//     // Assert both values (expected: 0.5 for each group)
//     assert(math.abs(kmeansCost - 1.0) < 1e-4)
//     assert(math.abs(surrogateCost - 1.0) < 1e-4)
//   }
// }
