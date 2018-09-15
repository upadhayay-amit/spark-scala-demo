package learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.approx_count_distinct
import org.apache.spark.sql.functions.{sum, count, avg, expr}

object aggrsparkdemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local").appName("spark aggr demo Application").getOrCreate()
    val path = args(0)

    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferschema","true")
      .load(path)
      .coalesce(4)
      .cache()

    df.createOrReplaceTempView("dfTable")
    df.printSchema()
    df.select(count("InvoiceNo")).show()
    df.select(approx_count_distinct("StockCode", 0.1)).show()
    df.select(
      count("Quantity").alias("total_transactions"),
      sum("Quantity").alias("total_purchases"),
      avg("Quantity").alias("avg_purchases"),
      expr("mean(Quantity)").alias("mean_purchases"))
      .selectExpr(
        "total_purchases/total_transactions",
        "avg_purchases",
        "mean_purchases").show()

  }

}
