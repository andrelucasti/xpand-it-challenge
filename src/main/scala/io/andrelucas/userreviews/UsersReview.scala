package io.andrelucas.userreviews

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.Double.NaN

case class UsersReview(df: DataFrame) {
  def fetchReviewsByApp(): DataFrame =
    df.select(col("App"), col("Sentiment_Polarity").cast("double"))
      .where(df("Sentiment_Polarity") =!= NaN or df("Sentiment_Polarity") =!= null)

  def fetchAverageSentimentPolarityByApp(): DataFrame = {
    fetchReviewsByApp()
      .groupBy(col("App"))
      .agg(
        avg(col("Sentiment_Polarity")).as("Average Sentiment Polarity")
      )
      .na.fill(0)
  }
}

object UsersReview {
  def schema: StructType = StructType(
    Array(
      StructField("App", StringType),
      StructField("Translated_Review", StringType),
      StructField("Sentiment", StringType),
      StructField("Sentiment_Polarity", StringType),
      StructField("Sentiment_Subjectivity", StringType),
    )
  )

  def path: String = scala.util.Properties.envOrElse("GOOGLE_PLAY_STORE_USER_REVIEWS_PATH", "src/main/resources/data/googleplaystore_user_reviews.csv")
}
