import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("LearnScalaSpark")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val lines = sc.textFile("C:\\Users\\Spencer Kasper\\Downloads\\setup.log")
    val words = lines.flatMap(line => line.split(' '))
    val wordsKVRdd = words.map(x => (x,1))
    val count = wordsKVRdd
      .reduceByKey((x,y) => x + y)
      .map(x => (x._2,x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)
    count.foreach(println)

    val helloWorldString = "Hello World!"
    print(helloWorldString)
  }

}