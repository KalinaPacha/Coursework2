import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

val sc = new SparkContext(new SparkConf())

val spark = new org.apache.spark.sql.SQLContext(sc)

val reviews_df = spark.read.option("header", true).option("inferSchema", true).option("sep", "\t").format("csv").load("reviews.csv").where(!(col("reviewText") === "None")) 

// Creating temp table
reviews_df.createOrReplaceTempView("reviews")

// calculating output
spark.sql("SELECT CATEGORIES, ROUND(AVG(RATING), 2) AVG_RATING FROM REVIEWS GROUP BY CATEGORIES").show(15, false)

