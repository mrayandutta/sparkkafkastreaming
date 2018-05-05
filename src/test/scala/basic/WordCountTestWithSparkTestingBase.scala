package basic

import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext

class WordCountTestWithSparkTestingBase extends FunSuite with SharedSparkContext {
  private val wordCount = new WordCount

  test("get word count rdd") {
    val result = wordCount.get(common.EnvironmentConstants.TestDataDirectoryRelativePath, sc)
    assert(result.take(2).length === 2)
  }
}
