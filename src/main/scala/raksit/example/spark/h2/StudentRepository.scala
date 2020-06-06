package raksit.example.spark.h2

import org.apache.spark.sql.{DataFrame, SQLContext}
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.auto._
import raksit.example.spark.config.Configuration

object StudentRepository {

  def findByGender(gender: String)(implicit sqlContext: SQLContext): DataFrame = {
    val configuration: ConfigReader.Result[Configuration] =
      ConfigSource.default.load[Configuration]

    configuration match {
      case Left(exception) =>
        exception.toList.foreach(println)
        sqlContext.emptyDataFrame

      case Right(configuration) =>
        val sqlQuery =
          s"""
             |select first_name, last_name from student
             |where gender = '$gender'
             |""".stripMargin

        sqlContext.read
          .format("jdbc")
          .option("url", configuration.database.url)
          .option("query", sqlQuery)
          .load()
    }
  }
}
