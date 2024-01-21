package io.andrelucas.playstore

import io.andrelucas.Operations
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max, regexp_replace}
import org.apache.spark.sql.types._

case class GooglePlayStoreApps(df: DataFrame) {

  val fetchBestApps: () => DataFrame = () => {
    Operations.fetchGreaterOrEqual(df, df("Rating"), 4.0)
  }

  val fetchFinalDataframe: () => DataFrame = () => {
    val colPriceEuro = regexp_replace(col("Price"), "\\$", "").cast("float") * 0.9
    val dfClear = transformCategoryAndGenres(df).na.fill(0)
      .groupBy(
        "App", "Rating", "Categories", "Size", "Installs", "Type", "Price", "Content Rating", "Genres", "Current Ver", "Android Ver"
      ).agg(
        max("Reviews").as("Reviews"),
        max("Last Updated").as("Last Updated"),
      )
      .withColumn("PriceInEuro", colPriceEuro)
      .drop("Price")
      .withColumnRenamed("PriceInEuro", "Price")
      .dropDuplicates("App")

    withSizeColumn(dfClear)
      .select(
        col("App"),
        col("Categories"),
        col("Rating"),
        col("Reviews"),
        col("Size"),
        col("Installs"),
        col("Type"),
        col("Price"),
        col("Content Rating"),
        col("Genres"),
        col("Last Updated"),
        col("Current Ver"),
        col("Android Ver"),
      )

  }

  private def transformCategoryAndGenres(df: DataFrame): DataFrame = {
    val groupByColumns = Array("App", "Rating", "Reviews", "Size", "Installs", "Type", "Price", "Content Rating", "Last Updated", "Current Ver", "Android Ver")
    val aggColumns = Array("Genres", "Category")

    Operations.transformSingleColumnToList(df, groupByColumns, aggColumns)
      .withColumnRenamed("Category", "Categories")
  }

  private def withSizeColumn(df: DataFrame): DataFrame = {
    val sizeInKToMB = regexp_replace(col("Size"), "k", "").cast("float") / 1000
    val sizeInMB = regexp_replace(col("Size"), "M", "").cast("float")

    val dfValid = df.where(col("Size").endsWith("k") or col("Size").endsWith("M"))

    val dfWithColumnInMB = dfValid.select("*").where(dfValid("Size").endsWith("M"))
      .withColumn("SizeInMB", sizeInMB)
      .drop("Size")
      .withColumnRenamed("SizeInMB", "Size")

    val dfWithColumnInK = dfValid.select("*").where(dfValid("Size").endsWith("k"))
      .withColumn("SizeInMB", sizeInKToMB)
      .drop("Size")
      .withColumnRenamed("SizeInMB", "Size")

    dfWithColumnInMB.union(dfWithColumnInK)
  }
}

object GooglePlayStoreApps {
  def schema: StructType = StructType(
    Array(
      StructField("App", StringType),
      StructField("Category", StringType),
      StructField("Rating", DoubleType),
      StructField("Reviews", LongType),
      StructField("Size", StringType),
      StructField("Installs", StringType),
      StructField("Type", StringType),
      StructField("Price", StringType),
      StructField("Content Rating", StringType),
      StructField("Genres", StringType),
      StructField("Last Updated", DateType),
      StructField("Current Ver", StringType),
      StructField("Android Ver", StringType),
    )
  )

  def path: String = scala.util.Properties.envOrElse("GOOGLE_PLAY_STORE_PATH", "src/main/resources/data/googleplaystore.csv")
}
