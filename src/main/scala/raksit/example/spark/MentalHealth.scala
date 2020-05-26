package raksit.example.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object MentalHealth extends InitSpark {

  def getTreatmentByGender(csvPath: String): DataFrame = {

    // Source: https://www.kaggle.com/osmi/mental-health-in-tech-survey
    val mentalHealthDataFrame: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NA")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
      .option("mode", "failfast")
      .csv(getClass.getResource(csvPath).toString)

    val treatmentAllGenderDataFrame: DataFrame = mentalHealthDataFrame.select(col("Gender"),
      when(col("treatment").equalTo("Yes"), 1).otherwise(0).alias("all-Yes"),
      when(col("treatment").equalTo("No"), 1).otherwise(0).alias("all-No"))

    val parseGenderUserDefinedFunction = udf(GenderParser.parse _)

    val treatmentByGenderDataFrame: DataFrame = treatmentAllGenderDataFrame.select(
      parseGenderUserDefinedFunction(col("Gender")).alias("Gender"),
      col("all-Yes"), col("all-No"))

    treatmentByGenderDataFrame.groupBy("Gender")
      .agg(sum("all-Yes").alias("Yes"),
        sum("all-No").alias("No"))
  }
}
