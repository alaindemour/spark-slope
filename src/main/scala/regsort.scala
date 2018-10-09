import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import metrics.RegSlope

// For this program to run  in local mode on a macbook
// you need the following to your bash profile so that enough memory is allocated
// export _JAVA_OPTIONS="-Xms512m -Xmx4g"


object RegSort {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("regsort")
      .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")


    val foodData = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("./small-food.csv")

    val slo = new RegSlope

    foodData
      .select("Area", "Item", "Element", "year", "quantity")
      .groupBy("Area", "Item","year")
      .agg(sum("quantity").as("quantity"))
      .groupBy("Area", "Item")
      .agg(slo(col("year"), col("quantity")).as("slope"))
      .na
      .drop()
      .sort(desc("slope")).show(30)

    spark.stop()
  }
}
