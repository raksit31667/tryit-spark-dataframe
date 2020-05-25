package raksit.example.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSuite

class MentalHealthTest extends FunSuite with DataFrameSuiteBase {

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

  test("should return dataframe with gender and total yes-no counts when getTreatmentByGender") {
    // When
    val actualDataFrame: DataFrame = MentalHealth.getTreatmentByGender

    // Then
    val expectedSchema: StructType = StructType(List(StructField("Gender", StringType), StructField("Yes", LongType), StructField("No", LongType)))
    val expectedData: Seq[Row] = Seq(Row("Female", 170L, 77L), Row("Male", 450L, 541L), Row("Transgender", 17L, 4L))
    val expectedDataFrame: DataFrame = spark.createDataFrame(sc.parallelize(expectedData), expectedSchema)
    assertDataFrameNoOrderEquals(expectedDataFrame, actualDataFrame)
  }
}
