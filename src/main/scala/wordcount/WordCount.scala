package wordcount
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount
{
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-testing-example")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String])
  {
    val input: RDD[String] = sc.textFile("src/main/resources/intro.txt")
    val countByWordRdd: RDD[(String, Int)] = WordCounter.count(input)
    countByWordRdd.foreach(println)
  }

}
