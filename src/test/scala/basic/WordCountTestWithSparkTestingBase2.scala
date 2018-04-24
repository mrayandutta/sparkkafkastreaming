package basic

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext
import com.holdenkarau.spark.testing.RDDComparisons

class WordCountTestWithSparkTestingBase2 extends FunSuite with SharedSparkContext with RDDComparisons{
  private val wordCount = new WordCount

  test("get word count rdd with comparison") {
    val expected = sc.textFile(common.EnvironmentConstants.TestDataDirectoryRelativePath)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)

    val result = wordCount.get(common.EnvironmentConstants.TestDataDirectoryRelativePath, sc)

    assert(compareRDD(expected, result).isEmpty)
  }
}