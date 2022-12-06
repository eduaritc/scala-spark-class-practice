
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.sql.Timestamp

//for DS we have to match the col ID's with the actual colname of our data source through a case class
case class ordersData(order_id:String, order_date:Timestamp, order_customer_id:Int, order_status:String)
object DSDemo extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkconf = new SparkConf().setMaster("local").setAppName("DFDemo").set("spark.driver.host", "localhost")
  sparkconf.set("spark.testing.memory", "2147480000")
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()
  // to get the headers recognize
  //to infer the type of the columns
  val orderDF = spark.read.option("header", true).option("inferSchema", true).csv("orders.csv")
  //  Doing the same with a dataset
  import spark.implicits._
  val orderDS = orderDF.as[ordersData]
//  orderDS.filter(x => x.order_customer_id> 10000).show(5)
  orderDS.filter(x => x.order_status == "CLOSED").show(5)
//  orderDS.show()
  // aggregation functions
  spark.stop()
}
