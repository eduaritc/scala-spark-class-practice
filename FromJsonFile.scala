package com.sparkbyexamples.spark.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object FromJsonFile {

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkconf = new SparkConf().setMaster("local").set("spark.driver.host", "localhost")
    sparkconf.set("spark.testing.memory", "2147480000")
    sparkconf.set("spark.app.name", "FromJsonFile")
    sparkconf.set("spark.master", "local[*]")

    val spark: SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExamples.com")
      .config(sparkconf)
      .getOrCreate()
    val sc = spark.sparkContext

    //read json file into dataframe
    val df = spark.read.json("zipcodes.json")
    df.printSchema()
    df.show(false)

    //read multiline json file
    val multiline_df = spark.read.option("multiline", "true")
      .json("multiline-zipcode.json")
    multiline_df.printSchema()
    multiline_df.show(false)


    //read multiple files
    val df2 = spark.read.json(
      "zipcodes1.json",
      "zipcodes2.json")
    df2.show(false)

    //read all files from a folder
    val df3 = spark.read.json("zipcodes_streaming/*")
    df3.show(false)

    //Define custom schema
    val schema = new StructType()
      .add("City", StringType, true)
      .add("Country", StringType, true)
      .add("Decommisioned", BooleanType, true)
      .add("EstimatedPopulation", LongType, true)
      .add("Lat", DoubleType, true)
      .add("Location", StringType, true)
      .add("LocationText", StringType, true)
      .add("LocationType", StringType, true)
      .add("Long", DoubleType, true)
      .add("Notes", StringType, true)
      .add("RecordNumber", LongType, true)
      .add("State", StringType, true)
      .add("TaxReturnsFiled", LongType, true)
      .add("TotalWages", LongType, true)
      .add("WorldRegion", StringType, true)
      .add("Xaxis", DoubleType, true)
      .add("Yaxis", DoubleType, true)
      .add("Zaxis", DoubleType, true)
      .add("Zipcode", StringType, true)
      .add("ZipCodeType", StringType, true)

    val df_with_schema = spark.read.schema(schema).json("zipcodes.json")
    df_with_schema.printSchema()
    df_with_schema.show(false)

    spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcode USING json OPTIONS (path 'zipcodes.json')")
    spark.sqlContext.sql("SELECT * FROM zipcode").show()

    //Write json file

    df2.write
      .json("zipcodes3.json")
  }
}