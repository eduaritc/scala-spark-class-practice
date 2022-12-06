import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object DFDemo extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkconf = new SparkConf().setMaster("local").setAppName("DFDemo").set("spark.driver.host", "localhost")
  sparkconf.set("spark.testing.memory", "2147480000")
//  val sparkconf = new SparkConf()
//  sparkconf.set("spark.app.name", "DFDemo")
  //setting our local machine as master and to use as many core as we got available
//  sparkconf.set("spark.master", "local[*]")

  //to create a context (1 per program)
//  val sc = new SparkContext("local[*]", appName = "DFDemo")
  //to get o create a session (could be more than 1)
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()
  //val colOder = "order_id,order_date,order_customer_id,order_status"
  //other way of giving the scheme to a dataframe
  val orderScheme = StructType(List(
    StructField ("orderid", IntegerType, true),
    StructField ("orderdate", TimestampType, true),
    StructField ("customerid", IntegerType, true),
    StructField ("orderstatus", StringType, true)
  ))


  // to get the headers recognize
  //to infer the type of the columns
  val orderDF = spark.read.option("header", true)
    //option("inferSchema", true).
  .schema(orderScheme).csv("orders.csv")

  print(orderDF.rdd.getNumPartitions)
  //to make queries on about the data on the dataframe
//  val processedDF = orderDF.where("customerid > 10000").select("orderid", "orderstatus", "customerid" )

  // aggregation functions
  val processedDF = orderDF.where("customerid > 10000")
    .select("orderid", "orderstatus", "customerid" ) //narrow transformation - map
  .groupBy("customerid").count()// wide transformation reduceBy
  processedDF.show()

//  Writing output data in a file
//  processedDF.write.format("CSV").mode(SaveMode.Overwrite).option("path", "output21").save()

//  Writing output data in a table
//    processedDF.write.format("CSV").mode(SaveMode.Overwrite).saveAsTable("order")
//    .mode(SaveMode.Overwrite).option("output21").save()
//  orderDF.show()
  spark.stop()
}
