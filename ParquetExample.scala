  package com.sparkbyexamples.spark.dataframe

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.SparkConf
  import org.apache.spark.sql.SparkSession

  object ParquetExample {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)
      val sparkconf = new SparkConf().setMaster("local").set("spark.driver.host", "localhost")
      sparkconf.set("spark.testing.memory", "2147480000")
      sparkconf.set("spark.app.name", "ParquetExample")
      sparkconf.set("spark.master", "local[*]")

      val spark: SparkSession = SparkSession.builder()
        .master("local[1]")
        .appName("SparkByExamples.com")
        .config(sparkconf)
        .getOrCreate()

      val data = Seq(("James ", "", "Smith", "36636", "M", 3000),
        ("Michael ", "Rose", "", "40288", "M", 4000),
        ("Robert ", "", "Williams", "42114", "M", 4000),
        ("Maria ", "Anne", "Jones", "39192", "F", 4000),
        ("Jen", "Mary", "Brown", "", "F", -1)
      )

      val columns = Seq("firstname", "middlename", "lastname", "dob", "gender", "salary")
      import spark.sqlContext.implicits._
      val df = data.toDF(columns: _*)

      df.show()
      df.printSchema()

      df.write
        .parquet("people.parquet")

      val parqDF = spark.read.parquet("people.parquet")
      parqDF.createOrReplaceTempView("ParquetTable")

      spark.sql("select * from ParquetTable where salary >= 4000").explain()
      val parkSQL = spark.sql("select * from ParquetTable where salary >= 4000 ")

      parkSQL.show()
      parkSQL.printSchema()

      df.write
        .partitionBy("gender", "salary")
        .parquet("people2.parquet")

      val parqDF2 = spark.read.parquet("people2.parquet")
      parqDF2.createOrReplaceTempView("ParquetTable2")

      val df3 = spark.sql("select * from ParquetTable2  where gender='M' and salary >= 4000")
      df3.explain()
      df3.printSchema()
      df3.show()

      val parqDF3 = spark.read
        .parquet("people2.parquet/gender=M")
      parqDF3.show()

    }
}
