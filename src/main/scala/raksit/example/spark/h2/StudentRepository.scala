package raksit.example.spark.h2

import org.apache.spark.sql.{DataFrame, SQLContext}

object StudentRepository {

  def findByGender(gender: String)(implicit sqlContext: SQLContext): DataFrame = {
    val sqlQuery =
      s"""
         |select first_name, last_name from student
         |where gender = '$gender'
         |""".stripMargin

    sqlContext.read
      .format("jdbc")
      .option("url", "jdbc:h2:mem:test;USER=sa;DB_CLOSE_DELAY=-1")
      .option("query", sqlQuery)
      .load()
  }
}
