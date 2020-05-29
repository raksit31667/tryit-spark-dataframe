package raksit.example.spark.h2

import java.nio.charset.StandardCharsets

import org.h2.tools.RunScript

object H2DatabaseTestPreparer {
  private val databaseUrl = "jdbc:h2:mem:test;USER=sa;DB_CLOSE_DELAY=-1"

  def createStudentTable(): Unit = {
    RunScript.execute(databaseUrl, "sa", "", getClass.getClassLoader.getResource("create_student_table.sql").getFile, StandardCharsets.UTF_8.toString, false)
  }

  def dropStudentTable(): Unit = {
    RunScript.execute(databaseUrl, "sa", "", getClass.getClassLoader.getResource("drop_student_table.sql").getFile, StandardCharsets.UTF_8.toString, false)
  }
}
