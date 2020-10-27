import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkSQLDataframe {

  def main(args: Array[String]): Unit = {
    val spark = new Spark("practiceJoinPurchasesAndAccountsWithDataFrames")
      .get()

    val purchasesDF = spark.read.parquet("parquet\\purchases.parquet")
    val accountsDF = spark.read.parquet("parquet\\accounts.parquet")

     purchasesDF
      .join(accountsDF, "accountId")
      .where(col("name") === "Spencer Kasper")
      .write.parquet("parquet\\just-spencer.parquet")
  }
}