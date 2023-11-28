import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{mean, stddev}
import org.scalatest.funsuite.AnyFunSuite

class MovieratingDatasetTest extends AnyFunSuite {

  def readRatingsCSV(spark: SparkSession, filePath: String): DataFrame = {
    // Read ratings.csv
    spark.read.option("header", "true").csv(filePath)
  }


  test("Test movie ratings analysis with merged data") {
    val spark = SparkSession.builder()
      .appName("MovieratingDatasetTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Replace these paths with your actual file paths

    val ratingFilePath = "/Users/mariagloriaraquelobono/Fall2023/Movie-Analyzer/src/main/resources/ratings.csv"

    val ratingsData = readRatingsCSV(spark, ratingFilePath)

    val calculatedStats = ratingsData.agg(mean("rating").as("MeanRating"), stddev("rating").as("StdDevRating")).head()
    val meanRating = calculatedStats.getAs[Double]("MeanRating")
    val stdDevRating = calculatedStats.getAs[Double]("StdDevRating")

    // Define expected values based on your test data
    val meanRatingExpected = 3.5280903543608817
    val stdDevRatingExpected = 1.0654427636662405

    assert(meanRating === meanRatingExpected)
    assert(stdDevRating === stdDevRatingExpected)

    spark.stop()
  }

  test("Test handling of empty ratings dataset") {
    val spark = SparkSession.builder()
      .appName("MovieratingDatasetTest")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Create an empty DataFrame to simulate an empty ratings dataset
    val emptyTestData = Seq.empty[(String, Double)].toDF("userId", "rating")

    val calculatedStats = emptyTestData.agg(mean("rating").as("MeanRating"), stddev("rating").as("StdDevRating")).head()
    val meanRating = calculatedStats.getAs[Double]("MeanRating")
    val stdDevRating = calculatedStats.getAs[Double]("StdDevRating")

    // Define expected values for an empty dataset
    val meanRatingExpected = 0.0 // Expected mean rating for an empty dataset
    val stdDevRatingExpected = 0.0 // Expected standard deviation for an empty dataset

    assert(meanRating === meanRatingExpected)
    assert(stdDevRating === stdDevRatingExpected)

    spark.stop()
  }

}



