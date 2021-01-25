/* Full exercise: https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
* Results are the same as in solutions.
*/

import org.apache.spark.sql.SparkSession

object WarmUp2 extends App {
  val spark = SparkSession.builder
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /* How many distinct products have been sold in each day? */

  val salesDF = spark
    .read
    .format("parquet")
    .load("./src/resources/sales_parquet")
    .selectExpr("product_id", "cast(date as date)")

  salesDF.createOrReplaceTempView("sales_view")

  val distinctProductsSoldDF = spark.sql("SELECT COUNT(DISTINCT product_id) AS product_id_count, date " +
    "FROM sales_view " +
    "GROUP BY date")

  distinctProductsSoldDF.show()
}
