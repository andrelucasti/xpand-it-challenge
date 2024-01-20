package io.andrelucas

import io.andrelucas.playstore.GooglePlayStoreApps
import io.andrelucas.userreviews.UsersReview

case class GooglePlayDataProcessor(googlePlayStore: GooglePlayStoreApps,
                                   usersReview: UsersReview,
                                   googleMetrics: GooglePlayMetrics) {

  def process(): List[FinalDataFrame] = {
    val dfBestApps = googlePlayStore.fetchBestApps()
    val dfAvgSentPolarity = usersReview.fetchAverageSentimentPolarityByApp()
    val dfFinalPlayStore = googlePlayStore.fetchFinalDataframe()
    val dfReviews = usersReview.fetchReviewsByApp()

    val dfGooglePlayMetrics = googleMetrics.fetchMetricsBy(dfFinalPlayStore, dfReviews)

    val dfGooglePlayStoreCleaned = dfFinalPlayStore.join(
      dfAvgSentPolarity,
      dfFinalPlayStore("App") === dfAvgSentPolarity("App"),
      "left"
    ).select(
      dfFinalPlayStore("App"),
      dfFinalPlayStore("Categories"),
      dfFinalPlayStore("Rating"),
      dfFinalPlayStore("Reviews"),
      dfFinalPlayStore("Size"),
      dfFinalPlayStore("Installs"),
      dfFinalPlayStore("Type"),
      dfFinalPlayStore("Price"),
      dfFinalPlayStore("Content Rating"),
      dfFinalPlayStore("Genres"),
      dfFinalPlayStore("Last Updated"),
      dfFinalPlayStore("Current Ver"),
      dfFinalPlayStore("Android Ver"),
      dfAvgSentPolarity("Average Sentiment Polarity")
    ).na.fill(0)

    val finalPath = scala.util.Properties.envOrElse("OUTPUT_PATH", "src/main/resources/finalData/")
    val bestApps = FinalDataFrame("best_apps", finalPath, CSV("§"), dfBestApps)
    val googlePlayCleaned = FinalDataFrame("googleplaystore_cleaned", finalPath, Parquet("gzip"), dfGooglePlayStoreCleaned)
    val googlePlayMetrics = FinalDataFrame("googleplaystore_metrics", finalPath, Parquet("gzip"), dfGooglePlayMetrics)

    List(bestApps, googlePlayCleaned, googlePlayMetrics)
  }
}
