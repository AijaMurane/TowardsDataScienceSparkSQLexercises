import org.apache.spark.sql.SparkSession

object ParquettoCSV extends App {
  val spark = SparkSession.builder
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.executor.memory", "500mb")
    .appName("Exercise1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val productsDF = spark.read.format("parquet").load("./src/resources/products_parquet")
  val salesDF = spark.read.format("parquet").load("./src/resources/sales_parquet")
  val sellersDF = spark.read.format("parquet").load("./src/resources/sellers_parquet")

  productsDF
    .coalesce(1)
    .write.option("header","true")
    .format("csv")
    .mode("overwrite")
    .save("./src/resources/productsCSV")

  salesDF
    .coalesce(1)
    .write.option("header","true")
    .format("csv")
    .mode("overwrite")
    .save("./src/resources/salesCSV")

  sellersDF
    .coalesce(1)
    .write.option("header","true")
    .format("csv")
    .mode("overwrite")
    .save("./src/resources/sellersCSV")
}
