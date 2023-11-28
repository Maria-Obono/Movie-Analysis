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
    val filePath = "/Users/mariagloriaraquelobono/Fall2023/Movie-Analyzer/src/main/resources/ratings.csv"
    val movieData = spark.read.option("header", "true").csv(filePath)

    // Calculate mean rating and standard deviation
    val meanRating = movieData.select(mean("rating")).first().getDouble(0)
    val stdDevRating = movieData.select(stddev("rating")).first().getDouble(0)

    println(s"Mean Rating: $meanRating")
    println(s"Standard Deviation of Rating: $stdDevRating")

    // Stop the SparkSession
    spark.stop()
  }

}
