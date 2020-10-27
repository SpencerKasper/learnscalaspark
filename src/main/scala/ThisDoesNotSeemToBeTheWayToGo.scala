import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json._

object ThisDoesNotSeemToBeTheWayToGo {
  def main(args: Array[String]): Unit = {
    println("start")
    val conf = new SparkConf().
      setMaster("local").
      setAppName("LearnScalaSpark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val purchaseJsonStrings = sc.textFile("json\\purchases.json")
    val accountJsonStrings = sc.textFile("json\\accounts.json")

    val purchases = purchaseJsonStrings.map(purchase => JSON.parseFull(purchase))
    val accounts = purchaseJsonStrings.map(account => JSON.parseFull(account))

    purchases.collect().foreach(println)
    println("end")
  }
}
