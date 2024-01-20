package io.andrelucas

import io.andrelucas.playstore.GooglePlayStoreApps
import io.andrelucas.userreviews.UsersReview
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {
  private val spark = SparkSession.builder()
    .appName("DataFrames")
    .config("spark.master", "local")
    .getOrCreate()

  private val dfUserReviews = spark
    .read
    .option("header", "true")
    .schema(UsersReview.schema)
    .csv(UsersReview.path)

  private val dfGooglePlayStore = spark
    .read
    .option("header", "true")
    .option("dateFormat", "MMMM d, yyyy")
    .schema(GooglePlayStoreApps.schema)
    .csv(GooglePlayStoreApps.path)

  private val dataProcessor = GooglePlayDataProcessor(
    GooglePlayStoreApps(dfGooglePlayStore),
    UsersReview(dfUserReviews),
    GooglePlayMetrics()
  )

  def main(args : Array[String]): Unit = {
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