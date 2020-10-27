import org.apache.spark.sql.SparkSession

object JoinPurchasesToAccountsUsingASqlQueryString {

  def main(args: Array[String]): Unit = {
    val spark = new Spark("sparkSqlTest").get()
    val purchasesDF = spark.read.parquet("parquet\\purchases.parquet")
    val accountsDF = spark.read.parquet("parquet\\accounts.parquet")

    purchasesDF.createOrReplaceTempView("purchases")
    accountsDF.createOrReplaceTempView("accounts")
    val purchasesJoinedToAccounts = spark.sql("SELECT p.purchaseId, p.purchaseTime, a.accountId, a.name FROM purchases p LEFT OUTER JOIN accounts a ON p.accountId = a.accountId")
    purchasesJoinedToAccounts.show()
  }

}