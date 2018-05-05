package streaming

import org.apache.spark._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue

object MapStateByKeySample {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "mapWithState", new SparkConf())

    val ssc = new StreamingContext(sc, batchDuration = Seconds(5))

    // checkpointing is mandatory
    ssc.checkpoint("C:/checkpoint/")

    val rdd = sc.parallelize(0 to 90).map(n => (n, n % 2 toString))
    import org.apache.spark.streaming.dstream.ConstantInputDStream
    val sessions = new ConstantInputDStream(ssc, rdd)

    import org.apache.spark.streaming.{State, StateSpec, Time}
    val updateState = (batchTime: Time, key: Int, value: Option[String], state: State[Int]) => {
      println(s">>> batchTime = $batchTime")
      println(s">>> key       = $key")
      println(s">>> value     = $value")
      println(s">>> state     = $state")
      val sum = value.getOrElse("").size + state.getOption.getOrElse(0)
      state.update(sum)
      Some((key, value, sum)) // mapped value
    }
    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = sessions.mapWithState(spec)

    mappedStatefulStream.print()

  }
}