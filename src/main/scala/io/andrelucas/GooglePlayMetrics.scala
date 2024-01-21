package io.andrelucas

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, count}

case class GooglePlayMetrics(){

  val fetchMetricsBy: (DataFrame, DataFrame) => DataFrame = (dfPlayStore, dfUserReviews) => {
    dfPlayStore
      .join(dfUserReviews, dfPlayStore("App") === dfUserReviews("App"), "left")
      .groupBy(col("Genres").as("Genre"))
      .agg(
        count(dfPlayStore("App")).as("Count"),
        avg(col("Rating")).as("Average_Rating"),
        avg(col("Sentiment_Polarity")).as("Average_Sentiment_Polarity")
      )
  }
}

