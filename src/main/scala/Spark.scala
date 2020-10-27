import org.apache.spark.sql.SparkSession

class Spark(val appName: String) {

  def get(): SparkSession = {
      SparkSession
        .builder()
        .appName(appName)
        .config("spark.master", "local")
        .getOrCreate()
  }
}
