package io.andrelucas

import io.andrelucas.playstore.GooglePlayStoreApps
import io.andrelucas.userreviews.UsersReview
import org.apache.spark.sql.SparkSession
import org.scalatest.wordspec.AnyWordSpec

class GooglePlayMetricsTest extends AnyWordSpec{

  private val spark = SparkSession.builder()
    .appName("DataFramesIntegrationTest")
    .config("spark.master", "local")
    .getOrCreate()

  "GooglePlayMetrics" should {
    "have Genre, Count, Average_Rating, Average_Sentiment_Polarity columns" in {
      val dfFinalPlayStore = GooglePlayStoreApps(
        spark
          .read
          .option("header", "true")
          .schema(GooglePlayStoreApps.schema)
          .csv(GooglePlayStoreApps.path)
      ).fetchFinalDataframe()

      val dfUserReviews = UsersReview(
        spark
          .read
          .option("header", "true")
          .schema(UsersReview.schema)
          .csv(UsersReview.path)
      ).fetchReviewsByApp()

      val subject = GooglePlayMetrics()
      val df = subject.fetchMetricsBy(dfFinalPlayStore, dfUserReviews)

      assert(df.columns.contains("Genre"))
      assert(df.columns.contains("Count"))
      assert(df.columns.contains("Average_Rating"))
      assert(df.columns.contains("Average_Sentiment_Polarity"))
    }
  }
}
