import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

val sc = new SparkContext(new SparkConf())

val spark = new org.apache.spark.sql.SQLContext(sc)

// Reading data set
val places = spark.read.option("header", true).option("inferSchema", true).option("sep", "\t").format("csv").load("places.csv")
val reviews = spark.read.option("header", true).option("inferSchema", true).option("sep", "\t").format("csv").load("reviews.csv")

// Creating temp table
places.createOrReplaceTempView("places")
reviews.createOrReplaceTempView("reviews")

// calculating output
spark.sql("SELECT PRICE, ROUND(AVG(RATING), 2) AVG_RATING FROM PLACES P INNER JOIN REVIEWS R ON P.gPlusPlaceId = R.gPlusPlaceId WHERE PRICE <> 'None' GROUP BY PRICE").show(5, false)
