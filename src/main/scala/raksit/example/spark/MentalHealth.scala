package raksit.example.spark

import org.apache.spark.sql.DataFrame

object MentalHealth extends InitSpark {

  def main(): DataFrame = {

    val mentalHealthDataFrame: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NA")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
      .option("mode", "failfast")
      .csv(getClass.getResource("/mental-health.csv").toString)
    mentalHealthDataFrame
  }
}
