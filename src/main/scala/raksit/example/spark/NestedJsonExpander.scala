package raksit.example.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.annotation.tailrec
import scala.io.Source

object NestedJsonExpander extends InitSpark {

  def getDataFrameFromNestedJson(jsonPath: String): DataFrame = {
    val nestedJsonDataFrame = sqlContext.read.json(
      sparkContext.parallelize(
        Source.fromInputStream(getClass.getResourceAsStream(jsonPath)).getLines().mkString :: Nil))
    nestedJsonDataFrame.show(truncate = false)
    expandNestedDataFrame(nestedJsonDataFrame)
  }

  @tailrec
  private def expandNestedDataFrame(nestedDataFrame: DataFrame): DataFrame = {
    var tempDataFrame = nestedDataFrame
    var selectClauseList: List[String] = List.empty[String]

    for (columnName <- tempDataFrame.schema.fieldNames) {
      tempDataFrame.schema(columnName).dataType match {
        case _: ArrayType =>
          tempDataFrame = tempDataFrame.withColumn(columnName, explode(col(columnName)))
          selectClauseList = selectClauseList :+ columnName
        case _: StructType =>
          for (fieldName <- tempDataFrame.schema(columnName).dataType.asInstanceOf[StructType].fieldNames) {
            selectClauseList = selectClauseList :+ s"$columnName.$fieldName"
          }
        case _ => selectClauseList = selectClauseList :+ columnName
      }
    }

    val expandedDataFrame = tempDataFrame.select(
      selectClauseList.map(name => col(name).alias(name.replace('.', '_'))): _*)
    expandedDataFrame.show(truncate = false)

    if (expandedDataFrame.schema.equals(nestedDataFrame.schema) && expandedDataFrame.except(nestedDataFrame).count() == 0) {
      return expandedDataFrame
    }

    expandNestedDataFrame(expandedDataFrame)
  }
}
