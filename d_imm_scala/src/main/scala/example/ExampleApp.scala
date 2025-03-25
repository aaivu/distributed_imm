package example

import org.apache.spark.sql.SparkSession

object SparkApp {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Spark Scala Demo")
      .master("local[*]") // run locally using all cores
      .getOrCreate()

    // Just print some basic info
    println("Spark version: " + spark.version)

    // Do a quick Spark job
    val data = Seq(("Alice", 29), ("Bob", 35), ("Cathy", 18))
    val df = spark.createDataFrame(data).toDF("name", "age")
    df.show()

    spark.stop()
  }
}
