
import java.io.File
import org.apache.avro.Schema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Spark Avro library example
 * Avro schema example
 * Avro file format
 *
 */
object AvroExample {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkconf = new SparkConf().setMaster("local").set("spark.driver.host", "localhost")
    sparkconf.set("spark.testing.memory", "2147480000")
    sparkconf.set("spark.app.name", "AvroExample")
    sparkconf.set("spark.master", "local[*]")

    val spark: SparkSession = SparkSession.builder().master("local[*]")
      .appName("SparkByExamples.com")
      .config(sparkconf)
      .getOrCreate()

    val data = Seq(("James ", "", "Smith", 2018, 1, "M", 3000),
      ("Michael ", "Rose", "", 2010, 3, "M", 4000),
      ("Robert ", "", "Williams", 2010, 3, "M", 4000),
      ("Maria ", "Anne", "Jones", 2005, 5, "F", 4000),
      ("Jen", "Mary", "Brown", 2010, 7, "", -1)
    )

    val columns = Seq("firstname", "middlename", "lastname", "dob_year",
      "dob_month", "gender", "salary")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns: _*)

    /**
     * Spark DataFrame Write Avro File
     */
    df.write.format("avro")
      .mode(SaveMode.Overwrite)
      .save("person.avro")

    /**
     * Spark DataFrame Read Avro File
     */
    spark.read.format("avro").load("person.avro").show()

    /**
     * Write Avro Partition
     */
    df.write.partitionBy("dob_year","dob_month")
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save("person_partition.avro")

    /**
     * Reading Avro Partition
     */
    spark.read
      .format("avro")
      .load("person_partition.avro")
      .where(col("dob_year") === 2010)
      .show()

    /**
     * Explicit Avro schema
     */
    val schemaAvro = new Schema.Parser()
      .parse(new File("person.avsc"))

    spark.read
      .format("avro")
      .option("avroSchema", schemaAvro.toString)
      .load("person.avro")
      .show()

    /**
     * Avro Spark SQL
     */
    spark.sqlContext.sql("CREATE TEMPORARY VIEW PERSON USING avro OPTIONS (path \"person.avro\")")
      spark.sqlContext.sql("SELECT * FROM PERSON").show()
  }
}

