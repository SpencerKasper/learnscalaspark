import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json._

object PurchasesByAccount {
  def main(args: Array[String]): Unit = {
    println("start")
    val conf = new SparkConf().
      setMaster("local").
      setAppName("LearnScalaSpark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val purchaseJsonStrings = sc.textFile("C:\\Users\\Spencer Kasper\\workspace\\learnscalaspark\\purchases.json")
    val accountJsonStrings = sc.textFile("C:\\Users\\Spencer Kasper\\workspace\\learnscalaspark\\accounts.json")

    val purchases = purchaseJsonStrings.map(purchase => JSON.parseFull(purchase))
    val accounts = purchaseJsonStrings.map(account => JSON.parseFull(account))
    

    purchases.collect().foreach(println)
    println("end")
  }
}
