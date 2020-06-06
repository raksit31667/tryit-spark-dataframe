package raksit.example.spark.kafka

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.FunSuite

class WordCounterTest extends FunSuite with StreamingSuiteBase {

  test("should return 1 pair when count given text is Foobar") {
    // Given
    val text = List(List("Foobar"))

    // When
    // Then
    val expected = List(List(("Foobar", 1L)))
    testOperation[String, (String, Long)](text, WordCounter.count _, expected, ordered = false)
  }

  test("should return 2 pairs when count given text is Hello world") {
    // Given
    val text = List(List("Hello world"))

    // When
    // Then
    val expected = List(List(("Hello", 1L), ("world", 1L)))
    testOperation[String, (String, Long)](text, WordCounter.count _, expected, ordered = false)
  }

  test("should return result correctly when count given text is empty") {
    // Given
    val text = List(List(""))

    // When
    // Then
    val expected = List(List())
    testOperation[String, (String, Long)](text, WordCounter.count _, expected, ordered = false)
  }

  test("should return result correctly when count given text is whitespace") {
    // Given
    val text = List(List(" "))

    // When
    // Then
    val expected = List(List())
    testOperation[String, (String, Long)](text, WordCounter.count _, expected, ordered = false)
  }
}
