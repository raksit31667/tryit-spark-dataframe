package raksit.example.spark.h2

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class StudentFilterAppTest extends FunSuite with DataFrameSuiteBase {

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    H2DatabaseTestPreparer.createStudentTable()
  }

  override def afterAll(): Unit = {
    H2DatabaseTestPreparer.dropStudentTable()
    super.afterAll()
  }

  test("should return only female dataframe when filter by gender given gender is female") {
    // When
    val actual: DataFrame = new StudentFilterApp().filterByGender("F")

    // Then
    val expected: DataFrame = Seq(("Cala", "Konrad")).toDF("first_name", "last_name")
    assertDataFrameNoOrderEquals(expected, actual)
  }

  test("should return only male dataframe when filter by gender given gender is male") {
    // When
    val actual: DataFrame = new StudentFilterApp().filterByGender("M")

    // Then
    val expected: DataFrame = Seq(("John", "Doe")).toDF("first_name", "last_name")
    assertDataFrameNoOrderEquals(expected, actual)
  }

  test("should return empty dataframe when filter by gender given gender is neither male nor female") {
    // When
    val actual: DataFrame = new StudentFilterApp().filterByGender("T")

    // Then
    assert(actual.isEmpty)
  }
}
