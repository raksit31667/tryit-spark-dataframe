package raksit.example.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.io.Source

object NestedJsonExpander extends InitSpark {

  def getDataFrameFromNestedJson(jsonPath: String): DataFrame = {
    var nestedJsonDataFrame = sqlContext.read.json(
      sparkContext.parallelize(
        Source.fromInputStream(getClass.getResourceAsStream(jsonPath)).getLines().mkString :: Nil))
    nestedJsonDataFrame.show(truncate = false)

    var isStillNested: Boolean = true

    while (isStillNested) {
      isStillNested = false
      var tempDataFrame: DataFrame = nestedJsonDataFrame
      var selectClauseList: List[String] = List.empty[String]

      for (columnName <- tempDataFrame.schema.fieldNames) {
        tempDataFrame.schema(columnName).dataType match {
          case _: ArrayType =>
            isStillNested = true
            tempDataFrame = tempDataFrame.withColumn(columnName, explode(col(columnName)))
            selectClauseList = selectClauseList :+ columnName
          case _: StructType =>
            isStillNested = true
            for (fieldName <- tempDataFrame.schema(columnName).dataType.asInstanceOf[StructType].fieldNames) {
              selectClauseList = selectClauseList :+ s"$columnName.$fieldName"
            }
          case _ => selectClauseList = selectClauseList :+ columnName
        }
      }

      nestedJsonDataFrame = tempDataFrame.select(
        selectClauseList.map(name => col(name).alias(name.replace('.', '_'))): _*)
      nestedJsonDataFrame.show(truncate = false)
    }

    nestedJsonDataFrame
  }
}
