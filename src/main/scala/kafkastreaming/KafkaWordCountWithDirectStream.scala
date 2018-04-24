package kafkastreaming

import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by Ayan  on 8/8/2017.
  */
object KafkaWordCountWithDirectStream
{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaWordCountWithDirectStream")
    val ssc = new StreamingContext(conf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // List of topics you want to listen for from Kafka
    val topicsSet = List("wordcount1").toSet
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val words = kafkaStream.flatMap(x =>  x._2.split(" "))

    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    kafkaStream.print()  //prints the stream of data received
    wordCounts.print()   //prints the wordcount result of the stream

    ssc.start()
    ssc.awaitTermination()
  }
}
