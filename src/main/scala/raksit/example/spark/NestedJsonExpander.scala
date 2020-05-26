package raksit.example.spark

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.io.Source

object NestedJsonExpander extends InitSpark {

  def getDataFrameFromNestedJson(jsonPath: String): DataFrame = {
    var nestedJsonDataFrame = sqlContext.read.json(
      sparkContext.parallelize(Source.fromInputStream(getClass.getResourceAsStream(jsonPath)).getLines().mkString :: Nil)
    )
    nestedJsonDataFrame.show(truncate = false)

    var nestedColumns = 1

    while (nestedColumns != 0) {
      var tempNestedColumns = 0

      for (columnName <- nestedJsonDataFrame.schema.fieldNames) {
        nestedJsonDataFrame.schema(columnName).dataType match {
          case _: ArrayType | _: StructType => tempNestedColumns += 1
          case _ => tempNestedColumns += 0
        }
      }

      if (tempNestedColumns != 0) {
        nestedJsonDataFrame = expandNestedColumn(nestedJsonDataFrame)
        nestedJsonDataFrame.show(truncate = false)
      }
      nestedColumns = tempNestedColumns
    }

    nestedJsonDataFrame
  }

  private def expandNestedColumn(dataFrame: DataFrame): DataFrame = {
    var tempDataFrame: DataFrame = dataFrame
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
    val columns: List[Column] = selectClauseList.map(name => col(name).alias(name.replace('.', '_')))
    tempDataFrame.select(columns: _*)
  }
}
