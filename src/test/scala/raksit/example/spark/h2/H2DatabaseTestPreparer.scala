package raksit.example.spark.h2

import java.nio.charset.StandardCharsets

import org.h2.tools.RunScript
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.auto._
import raksit.example.spark.config.Configuration

object H2DatabaseTestPreparer {

  def executeSqlFile(sqlFilePath: String): Unit = {
    val configuration: ConfigReader.Result[Configuration] =
      ConfigSource.default.load[Configuration]

    configuration match {
      case Left(exception) =>
        exception.toList.foreach(println)
        throw new RuntimeException("Test configuration is missing...")

      case Right(configuration) =>
        RunScript.execute(configuration.database.url, "sa", "",
          getClass.getClassLoader.getResource(sqlFilePath).getFile, StandardCharsets.UTF_8, false)
    }
  }
}
