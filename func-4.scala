import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

val sc = new SparkContext(new SparkConf())

val spark = new org.apache.spark.sql.SQLContext(sc)

val rating = 5.0

val reviews_df = spark.read.option("header", true).option("inferSchema", true).option("sep", "\t").format("csv").load("reviews.csv")
                           .where(col("rating") === rating) 

val reviews_df2 = reviews_df.where(!(col("reviewText") === "None"))

def createThreeWordsList = (text: String) => {
  val words = text.split(" ")
  
  val res = new StringBuilder()
  
  if (words.length < 3) {
    "None"
  }
  else {
    for (i <- 0 to words.length-3) {
      res ++= words(i) + " " + words(i+1) + " " + words(i+2)
      
      if (i != words.length-3) res ++= "~"
    }
    
    res.toString
  }
}

val createThreeWordsList_udf = spark.udf.register("createThreeWordsList",createThreeWordsList)

val reviews_df3 = reviews_df2.withColumn("reviewText", createThreeWordsList_udf(col("reviewText")))

val reviews_df4 = reviews_df3.withColumn("reviewText", split(col("reviewText"), "~"))
val reviews_df5 = reviews_df4.select(explode(col("reviewText")).alias("reviewText"))

// Forming final aggregated data
val reviews_df_final = reviews_df5.where(!(col("reviewText") === "None")).groupBy("reviewText").count().orderBy(desc("count")) 

reviews_df_final.show(20, false)
