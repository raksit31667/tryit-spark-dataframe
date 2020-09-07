package raksit.example.spark.deequ

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.profiles.ColumnProfilerRunner
import com.amazon.deequ.schema.{RowLevelSchema, RowLevelSchemaValidator}
import org.apache.spark.sql.DataFrame
import raksit.example.spark.InitSpark

object AmazonDeequExample extends InitSpark {

  /**
   * Column-level verification
   * @param dataFrame DataFrame
   */
  def verifyByColumns(dataFrame: DataFrame): Unit = {
    val verificationResult = VerificationSuite()
      .onData(dataFrame)
      .addCheck(
        Check(CheckLevel.Error, "unit testing my data")
          .isComplete("id")
          .isUnique("id")
          .isComplete("productName")
          .isComplete("priority")
          .isContainedIn("priority", Array("high", "low"))
          .isNonNegative("numViews"))
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the test, everything is fine!")
    } else {
      println("We found errors in the data:\n")

      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filter { _.status != ConstraintStatus.Success }
        .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
    }

    // Column statistics
    val columnProfilingResult = ColumnProfilerRunner()
      .onData(dataFrame)
      .run()
    val statusProfile = columnProfilingResult.profiles("priority")
    println("Value distribution in 'priority':")
    statusProfile.histogram.foreach {
      _.values.foreach {
        case (key, entry) => println(s"\t$key occurred ${entry.absolute} times (ratio is ${entry.ratio})")
      }
    }
  }

  /**
   * Row-level verification
   * @param dataFrame DataFrame
   */
  def verifyByRows(dataFrame: DataFrame): Unit = {
    val schema = RowLevelSchema()
      .withIntColumn("id", isNullable = false)
      .withStringColumn("priority", isNullable = false, matches = Option("high|low"))
      .withIntColumn("numViews", minValue = Option(0))

    val verificationResult = RowLevelSchemaValidator.validate(dataFrame, schema)
    verificationResult.invalidRows.show()
  }
}
