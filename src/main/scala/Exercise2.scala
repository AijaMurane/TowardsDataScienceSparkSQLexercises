/* Full exercise: https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
*/


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, avg}

object Exercise2 extends App {
  val spark = SparkSession.builder
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /* For each seller, what is the average % contribution of an order to the seller's daily quota?
  # Example
  If Seller_0 with `quota=250` has 3 orders:
  Order 1: 10 products sold
  Order 2: 8 products sold
  Order 3: 7 products sold

  The average % contribution of orders to the seller's quota would be:
  Order 1: 10/105 = 0.04
  Order 2: 8/105 = 0.032
  Order 3: 7/105 = 0.028

  Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333 */

  val salesDF = spark.read.format("parquet").load("./src/resources/sales_parquet")
  val sellersDF = spark.read.format("parquet").load("./src/resources/sellers_parquet")

  salesDF.createOrReplaceTempView("sales_view")
  sellersDF.createOrReplaceTempView("sellers_view")

  val contributionsDF = spark.sql("SELECT sales_view.order_id, sales_view.seller_id, (sales_view.num_pieces_sold / sellers_view.daily_target) AS contribution_to_daily_target " +
    "FROM sales_view " +
    "JOIN sellers_view " +
    "WHERE sales_view.seller_id = sellers_view.seller_id")

  contributionsDF
    .groupBy("seller_id")
    .avg("contribution_to_daily_target").alias("avg%contribution")
    .orderBy("seller_id")
    .show()
}
