import columns.{Accounts, Purchases}
import org.apache.spark.sql.functions.{col, desc, sum}

object GetSumOfPurchasesForAccount {

  def main(args: Array[String]): Unit = {
    val spark = new Spark("practiceJoinPurchasesAndAccountsWithDataFrames")
      .get()

    val purchasesDF = spark.read.parquet("parquet\\purchases.parquet")
    val prunedPurchasesDF = purchasesDF.select(col("accountId") as "p_accountId", col("amount"))

    val accountsDF = spark.read.parquet("parquet\\accounts.parquet")

    accountsDF
      .join(prunedPurchasesDF, prunedPurchasesDF("p_accountId") === accountsDF(Accounts.AccountId), "left_outer")
      .groupBy(Accounts.Name, Accounts.AccountId)
      .agg(sum(Purchases.Amount).alias(Purchases.Amount))
      .na.fill(0)
      .orderBy(desc(Purchases.Amount))
      .show()
  }
}