package io.andrelucas

import io.andrelucas.playstore.GooglePlayStoreApps
import io.andrelucas.userreviews.UsersReview
import org.apache.spark.sql.SparkSession

object ClusterDeployApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("xpand-it-challenge")
      .getOrCreate()

    val dfUserReviews = spark
      .read
      .option("header", "true")
      .schema(UsersReview.schema)
      .csv(UsersReview.path)

    val dfGooglePlayStore = spark
      .read
      .option("header", "true")
      .option("dateFormat", "MMMM d, yyyy")
      .schema(GooglePlayStoreApps.schema)
      .csv(GooglePlayStoreApps.path)

    val dataProcessor = GooglePlayDataProcessor(
      GooglePlayStoreApps(dfGooglePlayStore),
      UsersReview(dfUserReviews),
      GooglePlayMetrics()
    )

    dataProcessor.process()
      .foreach(finalDataFrame => {
        println(s"Dataframe: ${finalDataFrame.name}")
        finalDataFrame.df.printSchema()
        finalDataFrame.df.show(5)
        println(s"Saving in path: ${finalDataFrame.path}")

        finalDataFrame.fileType.write(finalDataFrame.name, finalDataFrame.path, finalDataFrame.df)
      })
  }
}
