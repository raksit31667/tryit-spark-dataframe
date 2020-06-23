package raksit.example.spark.order

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FunSuite

class OrderStreamingAppTest extends FunSuite with DataFrameSuiteBase {

  import spark.implicits._

  test("should return aggregated order with two orderIds when processOrder " +
    "given order 123456, and 987654 from clientId abcd") {
    // Given
    val inputStream = MemoryStream[Order]
    inputStream.addData(Order(123456, "abcd", 2, 100))
    inputStream.addData(Order(987654, "abcd", 3, 80))
    val input = inputStream.toDF()

    // When
    val output = OrderStreamingApp.processOrder(input)
    val actual = processData(output)

    // Then
    val expected = Seq((Array(123456, 987654), "abcd", 440L, true, true))
      .toDF("orderIds", "clientId", "subtotal", "_isValidOrderId", "_isValidPriceAndAmount")
    assertDataFrameDataEquals(expected.drop("orderIds"), actual.drop("orderIds"))
  }

  private def processData(streamingDataFrame: DataFrame): DataFrame = {
    streamingDataFrame.writeStream
      .format("memory")
      .queryName("Output")
      .outputMode(OutputMode.Update())
      .start()
      .processAllAvailable()

    spark.sql("select * from Output")
  }
}
