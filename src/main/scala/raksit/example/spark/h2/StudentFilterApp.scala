package raksit.example.spark.h2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import raksit.example.spark.InitSpark

class StudentFilterApp extends InitSpark {

  def filterByGender(gender: String): DataFrame = {
    val students = StudentRepository.findByGender(gender)(sqlContext)
    students.select(students.columns.map(columnName => col(columnName).as(columnName.toLowerCase())): _*)
  }
}
