package dimm.score

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object KMeansCost {

  def compute(assigned: RDD[(Int, Vector)]): Double = {
    assigned
      .groupByKey()
      .mapValues { points =>
        val pointList = points.toList
        val count = pointList.size
        if (count == 0) 0.0
        else {
          val dim = pointList.head.size
          val centerArray = new Array[Double](dim)

          // Sum all vectors
          pointList.foreach { vec =>
            vec.toArray.zipWithIndex.foreach { case (v, i) => centerArray(i) += v }
          }

          // Average the center
          for (i <- centerArray.indices) centerArray(i) /= count
          val center = Vectors.dense(centerArray)

          // Sum squared distances
          pointList.map(p => Vectors.sqdist(p, center)).sum
        }
      }
      .values
      .sum()
  }
}
