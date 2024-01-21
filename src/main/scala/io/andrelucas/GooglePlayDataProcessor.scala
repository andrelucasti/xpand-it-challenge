package io.andrelucas

import io.andrelucas.playstore.GooglePlayStoreApps
import io.andrelucas.userreviews.UsersReview
import org.apache.spark.sql.DataFrame

case class GooglePlayDataProcessor(googlePlayStore: GooglePlayStoreApps,
                                   usersReview: UsersReview,
                                   googleMetrics: GooglePlayMetrics) {

  def process(): List[FinalDataFrame] = {
    val dfBestApps = googlePlayStore.fetchBestApps()
    val dfAvgSentPolarity = usersReview.fetchAverageSentimentPolarityByApp()
    val dfFinalPlayStore = googlePlayStore.fetchFinalDataframe()
    val dfReviews = usersReview.fetchReviewsByApp()
    val dfGooglePlayMetrics = googleMetrics.fetchMetricsBy(dfFinalPlayStore, dfReviews)
    val dfGooglePlayStoreCleaned = dfPlayStoreCleanedWithAvgSentPolarity(dfFinalPlayStore, dfAvgSentPolarity)

    val finalPath = scala.util.Properties.envOrElse("OUTPUT_PATH", "src/main/resources/finalData/")
    val bestApps = FinalDataFrame("best_apps", finalPath, CSV("§"), dfBestApps)
    val googlePlayCleaned = FinalDataFrame("googleplaystore_cleaned", finalPath, Parquet("gzip"), dfGooglePlayStoreCleaned)
    val googlePlayMetrics = FinalDataFrame("googleplaystore_metrics", finalPath, Parquet("gzip"), dfGooglePlayMetrics)

    List(bestApps, googlePlayCleaned, googlePlayMetrics)
  }

  private val dfPlayStoreCleanedWithAvgSentPolarity: (DataFrame , DataFrame) => DataFrame = (dfPlayStore, dfAvgSentPolarity) => {
    dfPlayStore.join(
      dfAvgSentPolarity,
      dfPlayStore("App") === dfAvgSentPolarity("App"),
      "left"
    ).select(
      dfPlayStore("App"),
      dfPlayStore("Categories"),
      dfPlayStore("Rating"),
      dfPlayStore("Reviews"),
      dfPlayStore("Size"),
      dfPlayStore("Installs"),
      dfPlayStore("Type"),
      dfPlayStore("Price"),
      dfPlayStore("Content Rating"),
      dfPlayStore("Genres"),
      dfPlayStore("Last Updated"),
      dfPlayStore("Current Ver"),
      dfPlayStore("Android Ver"),
      dfAvgSentPolarity("Average Sentiment Polarity")
    ).na.fill(0)
  }
}
