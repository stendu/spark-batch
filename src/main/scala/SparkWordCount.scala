import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkWordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))
    val book = sc.textFile(args(0))
    val words = book.flatMap(str => str.split(" "))
    val cleanwords = words.filter(str1 => !str1.isEmpty())
    val wordcount = cleanwords.map(word=>(word,1))
    val totalwordcnt = wordcount.reduceByKey(_+_)
    val invertedcount = totalwordcnt.map{case(word,count) => (count,word)}
    val sortedkey = invertedcount.sortByKey(false)
    val top10words = sortedkey.take(10)
    top10words.foreach(println);
  }
}
