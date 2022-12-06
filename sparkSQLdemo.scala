import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparkSQLdemo extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkconf = new SparkConf().setMaster("local").setAppName("sparkSQLdemo").set("spark.driver.host", "localhost")
  sparkconf.set("spark.testing.memory", "2147480000")
  val spark = SparkSession.builder().config(sparkconf).getOrCreate()
  val orderDF = spark.read.option("header", true)
    .option("inferSchema", true)
    .csv("orders.csv")
  val customerDF = spark.read.option("header", true)
    .option("inferSchema", true)
    .csv("customers.csv")
  //this table is just for querying, stored in memory
  orderDF.createOrReplaceTempView("orders")
  customerDF.createOrReplaceTempView("customers")
  //any valid query
//  val res = spark.sql("select order_status, count(*) as totOrders from orders group by order_status")
  val res1 = spark.sql("select order_status, full_name, customers.order_id from customers inner join orders on customers.order_id = orders.order_id")
  res1.show()
}
