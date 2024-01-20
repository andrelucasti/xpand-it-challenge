package io.andrelucas

import org.apache.spark.sql.functions.{avg, col, collect_list, collect_set}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
object Operations {
  def fetchAverageGroupedBy(df: DataFrame,
                            columns: Array[String],
                            avgColumn: String,
                            alias: String): DataFrame =

    df.groupBy(columns.map(c => col(c)): _*)
      .agg(avg(avgColumn).as(alias))
      .na.fill(0, Seq(alias))

  def fetchGreaterOrEqual(df: DataFrame, column: Column, value: Double): DataFrame =
    df.filter(column >= value)

  def writeCsvFile(df: DataFrame, path: String, fileName: String):Unit =
    df.write
      .format("csv")
      .options(Map(
        "header" -> "true",
        "sep" -> ","
      ))
      .mode(SaveMode.Overwrite)
      .save(s"$path/$fileName")

  def transformSingleColumnToList(df: DataFrame,
                                  groupByColumn: Array[String],
                                  singleDataToCollection: Array[String]): DataFrame = {

    val gColumns = groupByColumn.map(g => col(g))
    val sColumns = singleDataToCollection.map(s => collect_set(s).as(s))

    df.groupBy(
        gColumns: _*
      )
      .agg(sColumns.head, sColumns.tail: _*)
  }
}