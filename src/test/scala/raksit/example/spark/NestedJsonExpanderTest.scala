package raksit.example.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class NestedJsonExpanderTest extends FunSuite with DataFrameSuiteBase {
  test("should return expanded dataframe when getDataFrameFromNestedJson given nested json path") {
    // When
    val actualDataFrame: DataFrame = NestedJsonExpander.getDataFrameFromNestedJson("/nested.json")

    // Then
    val expectedSchema: StructType = StructType(List(
      StructField("CHECK_Check1", LongType),
      StructField("CHECK_Check2", StringType),
      StructField("COL", LongType),
      StructField("DATA_Crate_key", StringType),
      StructField("DATA_Crate_value", StringType),
      StructField("DATA_MLrate", StringType),
      StructField("IFAM", StringType),
      StructField("KTM", LongType)
    ))
    val expectedData: Seq[Row] = Seq(
      Row(1L, "TWO", 21L, "k1", "v1", "31", "EQR", 1548176931466L),
      Row(1L, "TWO", 21L, "k2", "v2", "31", "EQR", 1548176931466L),
      Row(1L, "TWO", 21L, "k3", "v3", "33", "EQR", 1548176931466L),
      Row(1L, "TWO", 21L, "k4", "v4", "33", "EQR", 1548176931466L)
    )
    val expectedDataFrame: DataFrame = spark.createDataFrame(sc.parallelize(expectedData), expectedSchema)
    assertDataFrameNoOrderEquals(expectedDataFrame, actualDataFrame)
  }

  test("should return expanded dataframe when getDataFrameFromNestedJson given partially expanded json path") {
    // When
    val actualDataFrame: DataFrame = NestedJsonExpander.getDataFrameFromNestedJson("/partially_expanded.json")

    // Then
    val expectedSchema: StructType = StructType(List(
      StructField("CHECK_Check1", LongType),
      StructField("CHECK_Check2", StringType),
      StructField("COL", LongType),
      StructField("DATA_Crate_key", StringType),
      StructField("DATA_Crate_value", StringType),
      StructField("DATA_MLrate", StringType),
      StructField("IFAM", StringType),
      StructField("KTM", LongType)
    ))
    val expectedData: Seq[Row] = Seq(
      Row(1L, "TWO", 21L, "k1", "v1", "31", "EQR", 1548176931466L),
      Row(1L, "TWO", 21L, "k2", "v2", "31", "EQR", 1548176931466L),
      Row(1L, "TWO", 21L, "k3", "v3", "33", "EQR", 1548176931466L),
      Row(1L, "TWO", 21L, "k4", "v4", "33", "EQR", 1548176931466L)
    )
    val expectedDataFrame: DataFrame = spark.createDataFrame(sc.parallelize(expectedData), expectedSchema)
    assertDataFrameNoOrderEquals(expectedDataFrame, actualDataFrame)
  }

  test("should return same dataframe when getDataFrameFromNestedJson given already expanded json path") {
    // When
    val actualDataFrame: DataFrame = NestedJsonExpander.getDataFrameFromNestedJson("/already_expanded.json")

    // Then
    val expectedSchema: StructType = StructType(List(
      StructField("CHECK_Check1", LongType),
      StructField("CHECK_Check2", StringType),
      StructField("COL", LongType),
      StructField("DATA_Crate_key", StringType),
      StructField("DATA_Crate_value", StringType),
      StructField("DATA_MLrate", StringType),
      StructField("IFAM", StringType),
      StructField("KTM", LongType)
    ))
    val expectedData: Seq[Row] = Seq(
      Row(1L, "TWO", 21L, "k1", "v1", "31", "EQR", 1548176931466L),
      Row(1L, "TWO", 21L, "k2", "v2", "31", "EQR", 1548176931466L),
      Row(1L, "TWO", 21L, "k3", "v3", "33", "EQR", 1548176931466L),
      Row(1L, "TWO", 21L, "k4", "v4", "33", "EQR", 1548176931466L)
    )
    val expectedDataFrame: DataFrame = spark.createDataFrame(sc.parallelize(expectedData), expectedSchema)
    assertDataFrameNoOrderEquals(expectedDataFrame, actualDataFrame)
  }
}
