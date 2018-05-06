package characterstreaming

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.FunSuite


class CharacterStreamingTestWithSparkTestingTest extends FunSuite with StreamingSuiteBase {

  test("capitalize by window") {
    val input = List(List('a'), List('b'), List('c'), List('d'), List('e'))

    val slide1 = List('A', 'B')
    val slide2 = List('B', 'C', 'D')
    val expected = List(slide1, slide2)

    testOperation(input, StreamOperations.capitalizeWindowed, expected)
  }
}