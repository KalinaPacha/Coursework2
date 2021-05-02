import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

val sc = new SparkContext(new SparkConf())

val spark = new org.apache.spark.sql.SQLContext(sc)

// Reading data set
val reviews = spark.read.option("header", true).option("inferSchema", true).option("sep", "\t").format("csv").load("reviews.csv")
val places = spark.read.option("header", true).option("inferSchema", true).option("sep", "\t").format("csv").load("places.csv")

// Creating temp table
reviews.createOrReplaceTempView("reviews")
places.createOrReplaceTempView("places")

// Calculating Output
spark.sql("SELECT NAME, R.gPlusPlaceId ID, COUNT(*) COUNT FROM PLACES P INNER JOIN REVIEWS R ON P.gPlusPlaceId = R.gPlusPlaceId GROUP BY NAME, ID ORDER BY COUNT DESC").show(10, false)
