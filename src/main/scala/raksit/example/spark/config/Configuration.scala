package raksit.example.spark.config

case class Configuration(database: DatabaseConfiguration, kafka: KafkaConfiguration)
case class DatabaseConfiguration(url: String)
case class KafkaConfiguration(bootstrapServers: String, groupId: String)

