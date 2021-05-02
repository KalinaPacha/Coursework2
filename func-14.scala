import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

val sc = new SparkContext(new SparkConf())

val spark = new org.apache.spark.sql.SQLContext(sc)

// Reading data set
val reviews = spark.read.option("header", true).option("inferSchema", true).option("sep", "\t").format("csv").load("reviews.csv")

// Creating temp table
reviews.createOrReplaceTempView("reviews")

// Calculating output
spark.sql("SELECT RATING, ROUND(AVG(SIZE(SPLIT(REVIEWTEXT, ''))), 2) AVG_REVIEW_LENGTH FROM REVIEWS GROUP BY RATING").show()
