package raksit.example.spark.order

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class OrderAggregatorTest extends FunSuite with DataFrameSuiteBase {

  import spark.implicits._

  test("should return aggregated order with subtotal and validation flag true when collectOrdersByClientId given " +
    "client id and all validation flags true") {
    // Given
    val input = Seq((123456, "abcd", 2, 100, true, true), (987654, "abcd", 3, 80, true, true))
      .toDF("orderId", "clientId", "amount", "price", "_isValidOrderId", "_isValidPriceAndAmount")

    // When
    val actual = OrderAggregator.collectOrdersByClientId(input)

    // Then
    val expected = Seq((Array(123456, 987654), "abcd", 440L, true, true))
      .toDF("orderIds", "clientId", "subtotal", "_isValidOrderId", "_isValidPriceAndAmount")
    assertDataFrameDataEquals(expected.drop("orderIds"), actual.drop("orderIds"))
  }

  test("should return aggregated order with subtotal and validation flag false when collectOrdersByClientId given " +
    "client id and all validation flags false") {
    // Given
    val input = Seq((123456, "abcd", 1, 100, false, false), (987654, "abcd", 7, 20, false, false))
      .toDF("orderId", "clientId", "amount", "price", "_isValidOrderId", "_isValidPriceAndAmount")

    // When
    val actual = OrderAggregator.collectOrdersByClientId(input)

    // Then
    val expected = Seq((Array(123456, 987654), "abcd", 240L, false, false))
      .toDF("orderIds", "clientId", "subtotal", "_isValidOrderId", "_isValidPriceAndAmount")
    assertDataFrameDataEquals(expected.drop("orderIds"), actual.drop("orderIds"))
  }

  test("should return aggregated order with subtotal and validation flag false when collectOrdersByClientId given " +
    "client id and some validation flags false") {
    // Given
    val input = Seq((123456, "abcd", 2, 100, false, true), (987654, "abcd", 3, 80, true, false))
      .toDF("orderId", "clientId", "subtotal", "_isValidOrderId", "_isValidPriceAndAmount")

    // When
    val actual = OrderAggregator.collectOrdersByClientId(input)

    // Then
    val expected = Seq((Array(123456, 987654), "abcd", 440L, false, false))
      .toDF("orderIds", "clientId", "subtotal", "_isValidOrderId", "_isValidPriceAndAmount")
    assertDataFrameDataEquals(expected.drop("orderIds"), actual.drop("orderIds"))
  }
}
