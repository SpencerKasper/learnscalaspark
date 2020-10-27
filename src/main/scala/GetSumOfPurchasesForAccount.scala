import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, desc, max, sum}

object GetSumOfPurchasesForAccount {

  def main(args: Array[String]): Unit = {
    val spark = new Spark("practiceJoinPurchasesAndAccountsWithDataFrames")
      .get()

    val purchasesDF = spark.read.parquet("parquet\\purchases.parquet")
    val prunedPurchasesDF = purchasesDF.select(col("accountId") as "p_accountId", col("amount"))

    val accountsDF = spark.read.parquet("parquet\\accounts.parquet")

    accountsDF
      .join(prunedPurchasesDF, prunedPurchasesDF("p_accountId") === accountsDF("accountId"), "left_outer")
      .groupBy("name", "accountId")
      .agg(sum("amount").alias("amount"))
      .na.fill(0)
      .orderBy(desc("amount"))
      .show()
  }
}