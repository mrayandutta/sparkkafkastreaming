package kafkastreaming

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by Ayan on 8/8/2017.
  */
object KafkaWordCountWithReceiver
{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaWordCountWithReceiver")
    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("wordcount1" -> 5))
    val words = kafkaStream.flatMap(x =>  x._2.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    kafkaStream.print()  //prints the stream of data received
    wordCounts.print()   //prints the wordcount result of the stream

    //System.setProperty("hadoop.home.dir", "c://winutils")
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()


  }

}