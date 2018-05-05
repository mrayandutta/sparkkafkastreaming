package kafkastreaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import _root_.kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}


import scala.collection.mutable.Set

/**
  * Created by mrpiku2017 on 8/10/2017.
  */
object StatefulEventProcessor
{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("StatefulEventProcessor")
      .setMaster("local[*]")
      .set("spark.driver.memory", "2g")
      .set("spark.streaming.kafka.maxRatePerPartition", "50000")
      .set("spark.streaming.backpressure.enabled", "true")

    val windowSize = Seconds(10)        // 10 seconds
    val slidingInterval = Seconds(2)      // 2 seconds



    val ssc = new StreamingContext(conf, windowSize)
    ssc.checkpoint("C:/checkpoint/") // set checkpoint directory

    val sc = ssc.sparkContext
    sc.setLogLevel("ERROR")

    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    val topicName = "events"

    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "localhost:9092","auto.offset.reset" -> "smallest")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, scala.collection.immutable.Set(topicName))
    val nonFilteredEvents = kafkaStream.map((tuple) => createEvent(tuple._2))
    val events = nonFilteredEvents.filter((event) =>
    {
      event.highUtiization() && event.isTimeRelevant()
    })
    events.print(100)


    val groupedEvents = events.transform((rdd) => rdd.groupBy(_.instanceId))
    //mapWithState function
    val updateState = (batchTime: Time, key: Int, value: Option[Iterable[PerformanceEvent]], state: State[(Option[Long], Set[PerformanceEvent])]) => {
      if (!state.exists) state.update((None, Set.empty))

      var updatedSet = Set[PerformanceEvent]()
      value.get.foreach(updatedSet.add(_))

      //exclude non-relevant events
      state.get()._2.foreach((tempEvent) =>
      {
        if (tempEvent.isTimeRelevant()) updatedSet.add(tempEvent)
      })

      var lastAlertTime = state.get()._1

      //launch alert if no alerts launched yet or if last launched alert was more than 120 seconds ago
      if (updatedSet.size >= 2 && (lastAlertTime.isEmpty || !((System.currentTimeMillis() - lastAlertTime.get) <= 120000)))
      {
        lastAlertTime = Some(System.currentTimeMillis())
        //alert in json to be published to kafka
        print("ALERT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      }

      state.update((lastAlertTime, updatedSet))
      Some((key, updatedSet)) // mapped value
    }

    /*
    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = groupedEvents.mapWithState(spec)

    mappedStatefulStream.print()
*/
    ssc.start()
    ssc.awaitTermination()
  }

  def createEvent(strEvent: String): PerformanceEvent =
  {
    //print(s"Event String $strEvent")
    val eventData = strEvent.split('|')

    val instanceId = eventData(0).toInt
    val time = eventData(1).toLong
    val utilization = eventData(2).toDouble
    println(s"instanceId:$instanceId,time:$time,utilization:$utilization")

    new PerformanceEvent(instanceId, time, utilization)
  }

}

class PerformanceEvent (val instanceId: Int, val time: Long, val utilization: Double) extends Serializable
{
  val HighUtilization = 90.0
  val RelevantTime = 1        //time window in seconds in which events will be considered from
  def highUtiization() = utilization > HighUtilization
  def isTimeRelevant() = (System.currentTimeMillis() - time) <= RelevantTime * 1000
}