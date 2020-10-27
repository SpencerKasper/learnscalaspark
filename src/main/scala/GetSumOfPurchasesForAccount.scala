object GetSumOfPurchasesForAccount {

  def main(args: Array[String]): Unit = {
    val spark = new Spark("practiceJoinPurchasesAndAccountsWithDataFrames")
      .get()

    val purchasesDF = spark.read.parquet("parquet\\purchases.parquet")
    val accountsDF = spark.read.parquet("parquet\\accounts.parquet")

    purchasesDF
      .join(accountsDF, "accountId")
      .groupBy("accountId", "name")
      .sum("amount")
      .show()
  }
}