package raksit.example.spark

import org.apache.spark.sql.DataFrame

object MentalHealth extends InitSpark {

  def main(): DataFrame = {

    val mentalHealthDataFrame: DataFrame = spark.read.csv(getClass.getResource("/mental-health.csv").toString)
    mentalHealthDataFrame
  }
}
