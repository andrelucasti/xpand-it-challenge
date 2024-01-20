package io.andrelucas

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

class OperationsTest extends AnyFlatSpec {

  private val spark = SparkSession.builder()
    .appName("DataFramesTest")
    .config("spark.master", "local")
    .getOrCreate()

  it should "return the average of the a column grouped by another column" in {

    val dataFrames = Seq(
      Row("App1", 2.0),
      Row("App2", 3.0),
      Row("App1", 2.0),
      Row("App2", 3.0),
    )

    val schema = StructType(
      Array(
        StructField("App", StringType),
        StructField("Sentiment_Polarity", DoubleType),
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(dataFrames), schema)

    val dfAverage = Operations.fetchAverageGroupedBy(df, Array("App"),"Sentiment_Polarity", "Average_Sentiment_Polarity")

    assert(dfAverage.count() == 2)
    assert(dfAverage.filter(dfAverage("App") === "App1").first().getDouble(1) == 2)
    assert(dfAverage.filter(dfAverage("App") === "App2").first().getDouble(1) == 3)
  }

  it should "return the average of the a column grouped by another column with null values" in {

    val dataFrames = Seq(
      Row("App1", 2.0),
      Row("App2", 3.0),
      Row("App1", 2.0),
      Row("App2", 3.0),
      Row("App1", null),
      Row("App2", null),
    )

    val schema = StructType(
      Array(
        StructField("App", StringType),
        StructField("Sentiment_Polarity", DoubleType),
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(dataFrames), schema)

    val dfAverage = Operations.fetchAverageGroupedBy(df, Array("App"),"Sentiment_Polarity", "Average_Sentiment_Polarity")

    assert(dfAverage.count() == 2)
    assert(dfAverage.filter(dfAverage("App") === "App1").first().getDouble(1) == 2)
    assert(dfAverage.filter(dfAverage("App") === "App2").first().getDouble(1) == 3)
  }

  it should "return items with a 'ratting' greater or equal to 4.0 sorted in descending order." in {

    val dataFrames = Seq(
      Row("App1", 1.0),
      Row("App2", 2.0),
      Row("App3", 3.0),
      Row("App4", 4.0),
      Row("App5", 5.0),
      Row("App6", null),
      Row("App7", null),
    )

    val schema = StructType(
      Array(
        StructField("App", StringType),
        StructField("Rating", DoubleType),
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(dataFrames), schema)

    val dfResult = Operations.fetchGreaterOrEqual(df, df.col("Rating"), 4.0)

    assert(dfResult.count() == 2)
    assert(dfResult.filter(dfResult("App") === "App5").first().getDouble(1) == 5)
    assert(dfResult.filter(dfResult("App") === "App4").first().getDouble(1) == 4)
  }

  it should "convert item with column duplicated to a single column" in {

    val dataFrame = Seq(
      Row("App1", 1.0, "Category1", "Music"),
      Row("App1", 1.0, "Category2", "Music"),
      Row("App2", 1.0, "Category1", "Photoshop"),
      Row("App3", 1.0, "Category1", "Photoshop"),
      Row("App2", 1.0, "Category2", "Tattoo Photo"),
    )
    val schema = StructType(
      Array(
        StructField("App", StringType),
        StructField("Rating", DoubleType),
        StructField("Category", StringType),
        StructField("Genre", StringType),
      )
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(dataFrame), schema)
    val dfResult = Operations.transformSingleColumnToList(df, Array("App", "Rating"), Array("Category", "Genre"))

    assert(dfResult.select("App").distinct().count() == 3)
  }
}
