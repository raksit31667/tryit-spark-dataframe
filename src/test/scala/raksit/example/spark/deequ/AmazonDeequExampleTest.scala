package raksit.example.spark.deequ

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

class AmazonDeequExampleTest extends FunSuite with DataFrameSuiteBase {

  test("it should print failed constriant from verification result when run given test dataset") {
    // Given
    val rdd = spark.sparkContext.parallelize(Seq(
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12)))

    val data = spark.createDataFrame(rdd)

    // When
    AmazonDeequExample.run(data)
  }
}
