package raksit.example.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class MentalHealthTest extends FunSuite with SharedSparkContext {

  test("should contain 1259 records") {
    val dataFrame: DataFrame = MentalHealth.main()

    assert(dataFrame.count() === 1259)
  }
}
