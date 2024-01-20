package io.andrelucas.playstore

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.wordspec.AnyWordSpec

class GooglePlayStoreAppsTest extends AnyFlatSpec {

  private val spark = SparkSession.builder()
    .appName("DataFramesIntegrationTest")
    .config("spark.master", "local")
    .getOrCreate()

  private val subject = GooglePlayStoreApps(
    spark
      .read
      .option("header", "true")
      .schema(GooglePlayStoreApps.schema)
      .csv(GooglePlayStoreApps.path)
  )


  it should "return only apps with rating greater or equal to 4.0" in {
    val df_bestApps = subject.fetchBestApps()

    assert(df_bestApps.count() > 0)
    assert(df_bestApps.where(df_bestApps("Rating") < 4.0).count() == 0)

    assert(df_bestApps.where(df_bestApps("Rating") === 4.0).count() > 0)
    assert(df_bestApps.where(df_bestApps("Rating") > 4.0).count() > 0)
  }

  it should "reviews column to have default value equal 0" in {
    val df = subject.fetchFinalDataframe()

    assert(df.count() > 0)
    assert(df.where(df("Reviews") === 0).count() > 0)
    assert(df.where(df("Reviews") === null).count() == 0)
    assert(df.where(df("Reviews") > 0).count() > 0)
  }

  it should "have data frames with price greater than 0" in {
    val df = subject.fetchFinalDataframe()
    val dfResult = df.select("*").where(df("Type") === "Paid" and df("Price") > 0)

    assert(dfResult.count() > 0)
  }

  it should "have data frames with price equals 0" in {
    val df = subject.fetchFinalDataframe()
    val dfResult = df.select("*").where(df("Price") === 0)

    assert(dfResult.count() > 0)

  }

  it should "not have data frames with price less than 0" in {
    val df = subject.fetchFinalDataframe()
    val dfResult = df.select("*").where(df("Price") < 0)

    assert(dfResult.count() == 0)
  }
}

class GooglePlayDFServiceFieldTest extends AnyWordSpec {

  private val spark = SparkSession.builder()
    .appName("DataFramesIntegrationTest")
    .config("spark.master", "local")
    .getOrCreate()

  private val subject = GooglePlayStoreApps(
    spark
      .read
      .option("header", "true")
      .schema(GooglePlayStoreApps.schema)
      .csv(GooglePlayStoreApps.path)
  )

  private val df = subject.fetchFinalDataframe()

  "A final data frame " should {
    "have App Column with type string" in {
      assert(df.select("App").dtypes(0)._2 == "StringType")
    }

    "doesn't have duplicated App Column" in {
      assert(df.select("App").count() == df.select("App").distinct().count())
    }

    "have Categories Column with type Array of strings" in {
        df.printSchema()
        assert(df.select("Categories").dtypes(0)._2 == "ArrayType(StringType,false)")
      }

    "have Rating Column with type double" in {
        assert(df.select("Rating").dtypes(0)._2 == "DoubleType")
    }

    "have Reviews Column with type long" in {
      assert(df.select("Reviews").dtypes(0)._2 == "LongType")
    }

    "have Size Column with type double" in {
      assert(df.select("Size").dtypes(0)._2 == "DoubleType")
    }

    "have Installs Column with type string" in {
      assert(df.select("Installs").dtypes(0)._2 == "StringType")
    }

    "have Type Column with type string" in {
      assert(df.select("Type").dtypes(0)._2 == "StringType")
    }

    "have Price Column with type double" in {
      assert(df.select("Price").dtypes(0)._2 == "DoubleType")
    }

    "have Content Rating Column with type string" in {
      assert(df.select("Content Rating").dtypes(0)._2 == "StringType")
    }

    "have Genres Column with type Array of strings" in {
      assert(df.select("Genres").dtypes(0)._2 == "ArrayType(StringType,false)")
    }

    "have Last Updated Column with type date" in {
      assert(df.select("Last Updated").dtypes(0)._2 == "DateType")
    }

    "have Current Ver Column with type string" in {
      assert(df.select("Current Ver").dtypes(0)._2 == "StringType")
    }

    "have Android Ver Column with type string" in {
      assert(df.select("Android Ver").dtypes(0)._2 == "StringType")
    }
  }
}

