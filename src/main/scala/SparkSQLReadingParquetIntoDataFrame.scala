import org.apache.spark.sql.SparkSession

object SparkSQLReadingParquetIntoDataFrame {

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

    purchasesDF.createOrReplaceTempView("purchases")
    accountsDF.createOrReplaceTempView("accounts")
    val purchasesJoinedToAccounts = spark.sql("SELECT p.purchaseId, p.purchaseTime, a.accountId, a.name FROM purchases p LEFT OUTER JOIN accounts a ON p.accountId = a.accountId")
    purchasesJoinedToAccounts.show()
  }

}