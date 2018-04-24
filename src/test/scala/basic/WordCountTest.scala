package basic

import org.scalatest.{ BeforeAndAfterAll, FunSuite }
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class WordCountTest extends FunSuite with BeforeAndAfterAll {
  private var sparkConf: SparkConf = _
  private var sc: SparkContext = _

  override def beforeAll() {
    sparkConf = new SparkConf().setAppName("unit-testing").setMaster("local")
    sc = new SparkContext(sparkConf)
  }

  private val wordCount = new WordCount

  test("get word count rdd") {
    val result = wordCount.get(common.EnvironmentConstants.TestDataDirectoryRelativePath, sc)
    assert(result.take(2).length === 2)
  }

  override def afterAll() {
    sc.stop()
  }
}
