import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

val sc = new SparkContext(new SparkConf())

val spark = new org.apache.spark.sql.SQLContext(sc)

// Reading places data set
val places_df = spark.read.option("header", true).option("inferSchema", true).option("sep", "\t").format("csv").load("places.csv")

// Grouping the data
val output = places_df.groupBy("closed").count()

output.show()
