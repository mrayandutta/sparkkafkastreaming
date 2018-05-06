package characterstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Clock, Seconds, StreamingContext}
import org.apache.spark.{FixedClock, SparkConf, SparkContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class CharacterStreamingTest extends FlatSpec with Matchers with BeforeAndAfter with Eventually
{
  var sc:SparkContext = _
  var ssc: StreamingContext = _
  var fixedClock: FixedClock = _

  override implicit val patienceConfig = PatienceConfig(timeout = scaled(Span(1500, Millis)))

  before {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test-streaming")
      .set("spark.streaming.clock", "org.apache.spark.FixedClock")

    ssc = new StreamingContext(sparkConf, Seconds(1))
    sc = ssc.sparkContext
    fixedClock = Clock.getFixedClock(ssc)
  }

  after {
    ssc.stop(stopSparkContext = true, stopGracefully = false)
  }

  behavior of "stream transformation"

  it should "apply transformation" in {
    val inputData: mutable.Queue[RDD[Char]] = mutable.Queue()
    var outputCollector = ListBuffer.empty[Array[Char]]

    val inputStream = ssc.queueStream(inputData)
    val outputStream = StreamOperations.capitalizeWindowed(inputStream)

    outputStream.foreachRDD(rdd=> {outputCollector += rdd.collect()})

    ssc.start()

    inputData += sc.makeRDD(List('a'))
    wait1sec() // T = 1s

    inputData += sc.makeRDD(List('b'))
    wait1sec() // T = 2s

    assertOutput(outputCollector, List('A','B'))

    inputData += sc.makeRDD(List('c'))
    wait1sec() // T = 3s

    inputData += sc.makeRDD(List('d'))
    wait1sec() // T = 4s
    assertOutput(outputCollector, List('B', 'C', 'D'))

    // no more data ingestion
    // and wait until next slide
    wait1sec() // T = 5s
    wait1sec() // T = 6s
    assertOutput(outputCollector, List('D'))
  }

  def assertOutput(result: Iterable[Array[Char]], expected: List[Char]) =
    eventually {
      result.last.toSet should equal(expected.toSet)
    }

  def wait1sec(): Unit =
  {
    fixedClock.addTime(Seconds(1))
  }
}
