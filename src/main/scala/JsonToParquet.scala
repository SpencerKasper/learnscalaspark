import org.apache.spark.sql.SparkSession

object JsonToParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val fileDF = spark.read.json("json\\purchases.json")
    fileDF.write.parquet("parquet\\purchases.parquet")
  }
}
