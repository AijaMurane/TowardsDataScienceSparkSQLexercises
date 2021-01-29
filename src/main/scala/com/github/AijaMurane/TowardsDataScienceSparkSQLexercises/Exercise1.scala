/* Exercises: https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
* Results are the same as in solutions.
* Key salting is suggested: https://towardsdatascience.com/the-art-of-joining-in-spark-dcbd33d693c
* Another explanation about key salting: https://www.youtube.com/watch?v=d41_X78ojCg
* Key salting code: https://github.com/gjeevanm/SparkDataSkewness/blob/master/src/main/scala/com/gjeevan/DataSkew/RemoveDataSkew.scala
* Here is my solution using SQL query. This solution takes longer time than with key salting.
* Elapsed time: 125.5228673s
* */


package com.github.AijaMurane.TowardsDataScienceSparkSQLexercises

import org.apache.spark.sql.functions.{avg}
import org.apache.spark.sql.SparkSession

object Exercise1 extends App {
  val spark = SparkSession.builder
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /* What is the average revenue of the orders? */
  /* From exercise description: Each row in this table is an order and every order can contain only one product.*/

  val productsDF = spark
    .read
    .format("parquet")
    .load("./src/resources/products_parquet")
    .selectExpr("product_id", "cast(price as integer)")

  val salesDF = spark
    .read
    .format("parquet")
    .load("./src/resources/sales_parquet")
    .selectExpr("order_id", "product_id", "cast(num_pieces_sold as integer)")

  val t0 = System.nanoTime()

  salesDF.createOrReplaceTempView("sales_view")
  productsDF.createOrReplaceTempView("products_view")

  val revenueDF = spark.sql("SELECT sales_view.order_id, (sales_view.num_pieces_sold * products_view.price) AS revenue " +
    "FROM sales_view " +
    "JOIN products_view " +
    "WHERE sales_view.product_id = products_view.product_id")

  revenueDF.select(avg("revenue")).show()

  val t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0) / 10e8 + "s")
}
