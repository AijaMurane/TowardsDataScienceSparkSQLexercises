/* Full exercise: https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
* Results are the same as in solutions.
* However, it is suggested to use "key salting" for increasing performance whe tables are joined:
* https://towardsdatascience.com/the-art-of-joining-in-spark-dcbd33d693c
*/


import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

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

  val productsDF = spark.read.format("parquet").load("./src/resources/products_parquet")
  val salesDF = spark.read.format("parquet").load("./src/resources/sales_parquet")

  salesDF.createOrReplaceTempView("sales_view")
  productsDF.createOrReplaceTempView("products_view")

  val t0 = System.nanoTime()

  val revenueDF = spark.sql("SELECT sales_view.order_id, (sales_view.num_pieces_sold * products_view.price) AS revenue " +
    "FROM sales_view " +
    "JOIN products_view " +
    "WHERE sales_view.product_id = products_view.product_id")

  revenueDF.select(avg("revenue")).show()

  val t1 = System.nanoTime()
  println("Elapsed time: " + (t1 - t0) / 10e8 + "s")

  /* Using the "key salting" way. */

  val t0KS = System.nanoTime()

  /*
  def saltedJoin(df: DataFrame, buildDF: DataFrame, joinExpression: Column, joinType: String, salt: Int): DataFrame = {
    val tmpDF = buildDF
      .withColumn("slt_range", array(Range(0, salt).toList.map(lit): _*))
    val tableDF = tmpDF
      .withColumn("slt_ratio_s", explode(tmpDF("slt_range"))).drop("slt_range")
    val streamDF = df
      .withColumn("slt_ratio", monotonically_increasing_id % salt)
    val saltedExpr = streamDF("slt_ratio") === tableDF("slt_ratio_s") && joinExpression

    streamDF
      .join(tableDF, saltedExpr, joinType)
      .drop("slt_ratio_s")
      .drop("slt_ratio")
  }

  val joinedDF = productsDF
    .saltedJoin(salesDF, "left", 100)
    .show()*/

    val salt = 100

    val tmpDF = productsDF
      .withColumn("slt_range", array(Range(0, salt).toList.map(lit): _*))
    val tableDF = tmpDF
      .withColumn("slt_ratio_s", explode(tmpDF("slt_range"))).drop("slt_range")
    val streamDF = salesDF
      .withColumn("slt_ratio", monotonically_increasing_id % salt)
    //val joinExpression = "left"
    //val saltedExpr = streamDF("slt_ratio") === tableDF("slt_ratio_s") && joinExpression

  println("Salted table:")
    streamDF
      .join(tableDF, streamDF("slt_ratio") === tableDF("slt_ratio_s"), "left")
      .drop("slt_ratio_s")
      .drop("slt_ratio")
      .show()

  val t1t0KS = System.nanoTime()
  println("Elapsed time: " + (t1t0KS - t0KS)/10e8 + "s")

}
