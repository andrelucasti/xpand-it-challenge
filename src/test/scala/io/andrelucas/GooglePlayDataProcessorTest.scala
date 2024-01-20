package io.andrelucas

import io.andrelucas.playstore.GooglePlayStoreApps
import io.andrelucas.userreviews.UsersReview
import org.apache.spark.sql.SparkSession
import org.scalatest.wordspec.AnyWordSpec

class GooglePlayDataProcessorTest extends AnyWordSpec {
  private val spark = SparkSession.builder()
    .appName("DataFramesIntegrationTest")
    .config("spark.master", "local")
    .getOrCreate()

  private val playStoreApps = GooglePlayStoreApps(spark
    .read
    .option("header", "true")
    .schema(GooglePlayStoreApps.schema)
    .csv(GooglePlayStoreApps.path)
  )

  private val userReviews = UsersReview(
    spark
      .read
      .option("header", "true")
      .schema(UsersReview.schema)
      .csv(UsersReview.path)
  )

  private val appsByAverageSentimentPolarity = GooglePlayDataProcessor(playStoreApps, userReviews, GooglePlayMetrics())

  private val dfList = appsByAverageSentimentPolarity.process()
  
  "GooglePlayDataProcessor" should {
    "process 3 files" in {
      assert(dfList.size == 3)
    }

    "process best_apps.csv" in {
      assert(dfList.exists(p => p.name.equals("best_apps")))
    }

    "process googleplaystore_cleaned.parquet" in {
      assert(dfList.exists(p => p.name.equals("googleplaystore_cleaned")))
    }

    "process googleplaystore_metrics.parquet" in {
      assert(dfList.exists(p => p.name.equals("googleplaystore_metrics")))
    }
  }
}
