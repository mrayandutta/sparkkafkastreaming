package kafkastreaming

import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming._
import utility.LogUtil



/**
  * Created by mrpiku2017 on 8/9/2017.
  */
object KafkaWordCountStatefulWithDirectStreamMapWithState
{
  def main(args: Array[String]): Unit = {

    LogUtil.logger.error("$$$$$$$$$$$$$$$$$$$$  starting the applcation $$$$$$$$$$$$$$$$$$$$$$$$")

    val conf = new SparkConf().setMaster("local[1]").setAppName("KafkaWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    // List of topics you want to listen for from Kafka
    val topicsSet = List("wordcount1").toSet
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val words = kafkaStream.flatMap(x =>  x._2.split(" "))
    val wordDstream = words.map(x => (x, 1))
    //Pass key ,value and state ?
    val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      LogUtil.logger.error(s"word :$word,sum:$sum")
      val output = (word, sum)
      LogUtil.logger.error(s"output :$output")
      state.update(sum)
      output
    }

    val stateDstream = wordDstream.mapWithState(
      StateSpec.function(mappingFunc))//.initialState(initialRDD))
    stateDstream.print()

    //Line to add hadoop home dir
    //System.setProperty("hadoop.home.dir", "c://winutils")
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()


  }


}