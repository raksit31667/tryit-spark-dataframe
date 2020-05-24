package raksit.example.spark

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class MentalHealthTest extends FunSuite with SharedSparkContext {

  test("should return 170 yes when getTreatmentByGender given gender is female") {
    // When
    val actualDataFrame: DataFrame = MentalHealth.getTreatmentByGender

    // Then
    val actual = actualDataFrame.filter(row => row.getAs[String]("Gender").equals("Female"))
      .select("Yes").collect().toSeq
    assert(actual.head.get(0) === 170)
  }

  test("should return 77 no when getTreatmentByGender given gender is female") {
    // When
    val actualDataFrame: DataFrame = MentalHealth.getTreatmentByGender

    // Then
    val actual = actualDataFrame.filter(row => row.getAs[String]("Gender").equals("Female"))
      .select("No").collect().toSeq
    assert(actual.head.get(0) === 77)
  }

  test("should return 450 yes when getTreatmentByGender given gender is male") {
    // When
    val actualDataFrame: DataFrame = MentalHealth.getTreatmentByGender

    // Then
    val actual = actualDataFrame.filter(row => row.getAs[String]("Gender").equals("Male"))
      .select("Yes").collect().toSeq
    assert(actual.head.get(0) === 450)
  }

  test("should return 541 no when getTreatmentByGender given gender is male") {
    // When
    val actualDataFrame: DataFrame = MentalHealth.getTreatmentByGender

    // Then
    val actual = actualDataFrame.filter(row => row.getAs[String]("Gender").equals("Male"))
      .select("No").collect().toSeq
    assert(actual.head.get(0) === 541)
  }

  test("should return 17 yes when getTreatmentByGender given gender is transgender") {
    // When
    val actualDataFrame: DataFrame = MentalHealth.getTreatmentByGender

    // Then
    val actual = actualDataFrame.filter(row => row.getAs[String]("Gender").equals("Transgender"))
      .select("Yes").collect().toSeq
    assert(actual.head.get(0) === 17)
  }

  test("should return 4 no when getTreatmentByGender given gender is transgender") {
    // When
    val actualDataFrame: DataFrame = MentalHealth.getTreatmentByGender

    // Then
    val actual = actualDataFrame.filter(row => row.getAs[String]("Gender").equals("Transgender"))
      .select("No").collect().toSeq
    assert(actual.head.get(0) === 4)
  }
}
