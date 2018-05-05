package wordcount

import com.holdenkarau.spark.testing.{RDDComparisons,SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class WordCounterWithSparkTestingTest extends FlatSpec with Matchers with SharedSparkContext with RDDComparisons
{

  behavior of "Words counter"

  it should "count words in a text" in {
    val text =
      """Hello Spark
        |Hello world
      """.stripMargin

    val inputRDD: RDD[String] = sc.parallelize(List(text))
    val expectedRDD: RDD[(String, Int)] = sc.parallelize(List(("Hello", 2), ("Spark", 1), ("world", 1)))

    val resultRDD: RDD[(String, Int)] = WordCounter.count(inputRDD)
    assert(None === compareRDD(expectedRDD, resultRDD)) // succeed
    //assert(None === compareRDDWithOrder(expectedRDD, resultRDD)) // Fail

    assertRDDEquals(expectedRDD, resultRDD) // succeed
    //assertRDDEqualsWithOrder(expectedRDD, resultRDD) // Fail
  }
}