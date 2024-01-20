package io.andrelucas.userreviews

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

import scala.Double.NaN

class UsersReviewIntegrationTest extends AnyFlatSpec {
  private val spark = SparkSession.builder()
    .appName("DataFramesIntegrationTest")
    .config("spark.master", "local")
    .getOrCreate()

  private val subject = UsersReview(spark
    .read
    .schema(UsersReview.schema)
    .option("header", "true")
    .csv(UsersReview.path))

  it should "return the Sentiment Polarity grouped by App name" in {
    val df = subject.fetchAverageSentimentPolarityByApp()

    assert(df.count() > 0)
    assert(df.where(df("Average Sentiment Polarity") === NaN).count() == 0)
    assert(df.where(df("Average Sentiment Polarity") =!= NaN).count() > 0)
  }
}
