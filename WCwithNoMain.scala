
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object WCwithNoMain  extends App {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val conf = new SparkConf().setMaster("local").setAppName("WC7").set("spark.driver.host", "localhost")
      conf.set("spark.testing.memory", "2147480000")
      //    val sc = new SparkContext(conf)

      val sc = new SparkContext("local[*]", "wordcount")
       //read from file
      val rdd1 = sc.textFile("sample.txt")
          //using anonymous holder
      //    //one input row will give multiple output rows
          val rdd2 = rdd1.flatMap(x => x.split(" "))
      //    //one input row will give one output row only
          val rdd3 = rdd2.map(x => (x, 1))
      //    //take two rows , and does aggregation and returns one row
          val rdd4 = rdd3.reduceByKey((x, y) => x + y)
      //    // print the output
          rdd4.collect.foreach(println)


      /*
          //one input row will give one output row only
          val rdd3 = rdd2.map(x => (x, 1))
          //take two rows , and does aggregation and returns one row
          val rdd4 = rdd3.reduceByKey((x, y) => x + y)

          //action

          rdd4.collect.foreach(println)
          */
      //    scala.io.StdIn.readLine()
//      val rdd2 = rdd1.flatMap(x => x.split(" ")).map(x => x.toLowerCase()).map(x => (x, 1)).reduceByKey((x, y) => x + y)
//      rdd2.collect.foreach(println)

      val res = rdd2.collect()
}
