/* Exercises: https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
* Key salting is suggested: https://towardsdatascience.com/the-art-of-joining-in-spark-dcbd33d693c
* Another explanation about key salting: https://www.youtube.com/watch?v=d41_X78ojCg
* Key salting code: https://github.com/gjeevanm/SparkDataSkewness/blob/master/src/main/scala/com/gjeevan/DataSkew/RemoveDataSkew.scala
* */

/* Here I try key salting only for product_id = 0 */

package com.github.AijaMurane.TowardsDataScienceSparkSQLexercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, avg, col, concat, explode, floor, lit, monotonically_increasing_id, rand}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ArrayBuffer

object Exercise1_keySalting1Product extends App {

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

  val t0KS = System.nanoTime()

  val nSaltBins = 100

  val salesDFkeySalted = salesDF
      .where(col("product_id") === "0")
      .withColumn("product_id", concat(
          salesDF.col("product_id"), lit("_"), lit(floor(rand * nSaltBins))))
    val productsDFkeySalted = productsDF
      .where(col("product_id") === "0")
      .withColumn("explodedCol",
        explode(
          array((0 to nSaltBins).map(lit(_)): _ *)
        ))

  salesDFkeySalted
    .join(productsDFkeySalted, salesDFkeySalted.col("product_id")<=> concat(productsDFkeySalted.col("product_id"),lit("_"),productsDFkeySalted.col("explodedCol")))
    .selectExpr("price*num_pieces_sold AS revenue")
    .select(avg("revenue")) //FIXME the average value is not correct. Probably problem with salting keys for 1 product.
    .show()

  val t1t0KS = System.nanoTime()
  println("Elapsed time: " + (t1t0KS - t0KS) / 10e8 + "s")
}
