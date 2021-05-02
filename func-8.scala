import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

val sc = new SparkContext(new SparkConf())

val spark = new org.apache.spark.sql.SQLContext(sc)

val reviews_df = spark.read.option("header", true).option("inferSchema", true).option("sep", "\t").format("csv").load("reviews.csv").where(!(col("reviewText") === "None")) 

// Selecting required cols
val reviews_df2 = reviews_df.select(col("gPlusPlaceId"), col("gPlusUserId"), col("reviewText"))

// Splitting text to words list
val reviews_df3 = reviews_df2.withColumn("reviewText", split(col("reviewText"), " "))

// Exploding list data
val reviews_df4 = reviews_df3.select(col("gPlusPlaceId"), col("gPlusUserId"), explode(col("reviewText")).alias("word"))

// Concatenating user and place id
val reviews_df5 = reviews_df4.select(col("word"), concat(col("gPlusPlaceId"), 
                                                    lit("_"), 
                                                    col("gPlusUserId")).alias("idx"))

// Forming final aggegrated data frame
val reviews_df6 = reviews_df5.groupBy("word").agg(concat_ws("||", collect_list(col("idx"))).alias("idx"))   

reviews_df6.show(10, false)
