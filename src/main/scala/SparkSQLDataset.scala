import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkSQLDataset {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", true)
    import spark.implicits._
    val purchasesDF = spark.read.parquet("purchases.parquet")
    val accountsDF = spark.read.parquet("accounts.parquet")

    purchasesDF
      .join(accountsDF, "accountId")
      .where(col("name") === "Spencer Kasper")
      .write.parquet("just-spencer.parquet")
  }
}