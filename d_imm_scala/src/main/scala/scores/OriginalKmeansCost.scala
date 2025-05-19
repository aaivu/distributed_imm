
package dimm.score

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

object OriginalKMeansCost {

  /**
   * Compute the original KMeans cost (sum of squared distances) from the cluster assignments.
   *
   * @param clusteredDF DataFrame with "features" and "prediction" columns
   * @param clusterCenters Array of cluster centers
   * @param spark SparkSession (for implicits)
   * @return total squared distance (KMeans cost)
   */
  def compute(clusteredDF: DataFrame, clusterCenters: Array[Vector])(implicit spark: SparkSession): Double = {
    import spark.implicits._

    val computeDistance = udf { (features: Vector, prediction: Int) =>
      Vectors.sqdist(features, clusterCenters(prediction))
    }

    val costDF = clusteredDF.withColumn("sqDist", computeDistance($"features", $"prediction"))
    costDF.selectExpr("sum(sqDist)").as[Double].first()
  }
}
