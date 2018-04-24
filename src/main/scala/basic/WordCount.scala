package basic

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import common.EnvironmentConstants

class WordCount
{
  def get(url: String, sc: SparkContext): RDD[(String, Int)] = {
    val lines = sc.textFile(url)
    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
  }
}

object WordCount{
  def main(args: Array[String]): Unit =
  {
    print("Hello world start !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ")
    val sc = new SparkContext( "local", "SparkPi", System.getenv("SPARK_HOME"), Nil, Map(), Map())
    val lines = new WordCount().get(EnvironmentConstants.TestDataDirectoryRelativePath,sc)
    print(lines.count())
    print("Hello world end !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ")
  }
}