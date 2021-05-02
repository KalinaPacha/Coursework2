import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

val sc = new SparkContext(new SparkConf())

val spark = new org.apache.spark.sql.SQLContext(sc)

// Reading data set
val places = spark.read.option("header", true).option("inferSchema", true).option("sep", "\t").format("csv").load("places.csv")

// Creating temp table
places.createOrReplaceTempView("places")

// calculating output
spark.sql("SELECT PRICE, COUNT(*) COUNT FROM PLACES WHERE CLOSED = 'false' AND PRICE <> 'None' GROUP BY PRICE").show(5, false)
