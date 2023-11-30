import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{mean, stddev}

object MovieratingDataset {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("MovieratingDataset")
      .master("local[*]") // Change this to your cluster setup
      .getOrCreate()

    // Read the CSV file into a DataFrame
    val filePath = "/Users/mariagloriaraquelobono/Fall2023/Movie-Analyzer/src/main/resources/ratings_small.csv"
    val movieData = spark.read.option("header", "true").csv(filePath)

    // Calculate mean rating and standard deviation
    val meanRating = BigDecimal(movieData.select(mean("rating")).first().getDouble(0)).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
    val stdDevRating = BigDecimal(movieData.select(stddev("rating")).first().getDouble(0)).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble

    println(s"Mean Rating: $meanRating")
    println(s"Standard Deviation of Rating: $stdDevRating")

    // Stop the SparkSession
    spark.stop()
  }

}
