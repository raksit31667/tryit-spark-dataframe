package raksit.example.spark.order

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class OrderIdValidatorTest extends FunSuite with DataFrameSuiteBase {

  import spark.implicits._

  test("should return dataframe with validation flag true when addValidOrderIdFlag given valid order id") {
    // Given
    val input = Seq((123456, "abcd", 2, 100)).toDF("orderId", "clientId", "amount", "price")

    // When
    val actual = OrderIdValidator.addValidOrderIdFlag().apply(input)

    // Then
    val expected = Seq((123456, "abcd", 2, 100, true)).toDF("orderId", "clientId", "amount", "price", "_isValidOrderId")
    assertDataFrameDataEquals(expected, actual)
  }

  test("should return dataframe with validation flag false when addValidOrderIdFlag given invalid order id") {
    // Given
    val input = Seq((234567, "abcd", 2, 100)).toDF("orderId", "clientId", "amount", "price")

    // When
    val actual = OrderIdValidator.addValidOrderIdFlag().apply(input)

    // Then
    val expected = Seq((234567, "abcd", 2, 100, false)).toDF("orderId", "clientId", "amount", "price", "_isValidOrderId")
    assertDataFrameDataEquals(expected, actual)
  }

  test("should return dataframe with validation flag false when addValidOrderIdFlag given null order id") {
    // Given
    val input = Seq((null, "abcd", 2, 100)).toDF("orderId", "clientId", "amount", "price")

    // When
    val actual = OrderIdValidator.addValidOrderIdFlag().apply(input)

    // Then
    val expected = Seq((null, "abcd", 2, 100, false)).toDF("orderId", "clientId", "amount", "price", "_isValidOrderId")
    assertDataFrameDataEquals(expected, actual)
  }
}
