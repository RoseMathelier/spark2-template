package sql_practice

import spark_helpers.SparkSessionHelper
import org.apache.spark.sql.functions._

object examples {

  def exec1(): Unit={
    val spark = SparkSessionHelper.getSparkSession()

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")

    toursDF.show()

    //Question #1 - how many levels of difficulties ?
    val nbDiff = toursDF
      .groupBy("tourDifficulty")
      .count()
    nbDiff.show()

    //Question #2 - what is the min/max/avg price of all tour packages
    val statPrice = toursDF
      .agg(avg("tourPrice"), min("tourPrice"), max("tourPrice"))
    statPrice.show()

    //Question #3 - what is the min/max/avg price for each level of difficulty ?
    val statPriceByLevel = toursDF
      .groupBy("tourDifficulty")
      .agg(avg("tourPrice"), min("tourPrice"), max("tourPrice"))
    statPriceByLevel.show()

    //Question #4 - what are the min/max/avg price and length for each level of difficulty ?
    val statPriceLengthByLevel = toursDF
      .groupBy("tourDifficulty")
      .agg(avg("tourPrice"), min("tourPrice"), max("tourPrice"), avg("tourLength"), min("tourLength"), max("tourLength"))
    statPriceLengthByLevel.show()

    //Question #5 - display the top 10 tourTags
    val tags = toursDF
      .withColumn("tourTags", explode(toursDF("tourTags")))
      .groupBy("tourTags")
      .count()
      .orderBy(desc("count"))
    tags.show(10)

    //Question #6 - relationship between top 10 "tourTags" and "tourDifficulty"
    val tags2 = toursDF
      .withColumn("tourTags", explode(toursDF("tourTags")))
      .groupBy("tourTags", "tourDifficulty")
      .count()
      .orderBy(desc("count"))
    tags2.show(10)

    //Question #7 - min/max/average price ib the previous relationship
    val statTags = toursDF
      .withColumn("tourTags", explode(toursDF("tourTags")))
      .groupBy("tourTags", "tourDifficulty")
      .agg(avg("tourPrice"), min("tourPrice"), max("tourPrice"))
      .orderBy(desc("avg(tourPrice)"))
    statTags.show(10)

  }

}
