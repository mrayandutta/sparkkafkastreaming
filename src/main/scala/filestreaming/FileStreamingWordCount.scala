package filestreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utility.LogUtil

object FileStreamingWordCount
{
  def main(args: Array[String]): Unit = {
    LogUtil.logger.error("$$$$$$$$$$$$$$$$$$$$  starting the applcation FileStreamingWordCount $$$$$$$$$$$$$$$$$$$$$$$$")
    val conf = new SparkConf().setMaster("local[*]").setAppName("FileStreamingWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    val directory = "c://data"
    val lines  = ssc.textFileStream(directory)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    lines.print()
    wordCounts.print()
    LogUtil.logger.error(wordCounts)
    ssc.start()
    ssc.awaitTermination()
  }
}