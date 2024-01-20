package io.andrelucas

import org.apache.spark.sql.{DataFrame, SaveMode}

trait FileType {
  def write(name:String, path: String, df: DataFrame): Unit
}
case class CSV(delimiter: String) extends FileType {
  override def write(name: String, path: String, df: DataFrame): Unit =
    df.write
      .format("csv")
      .options(Map(
        "header" -> "true",
        "sep" -> delimiter
      ))
      .mode(SaveMode.Overwrite)
      .save(s"$path/$name")
}
case class Parquet(compressionType: String) extends FileType {
  override def write(name: String, path: String, df: DataFrame): Unit =
    df.write
      .format("parquet")
      .options(
        Map(
          "compression"-> compressionType
        )
      )
      .mode(SaveMode.Overwrite)
      .save(s"$path/$name")
}

case class FinalDataFrame(name: String,
                          path: String,
                          fileType: FileType,
                          df: DataFrame) {

}
