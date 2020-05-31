package raksit.example.spark.h2

case class Configuration(database: DatabaseConfiguration)
case class DatabaseConfiguration(url: String)

