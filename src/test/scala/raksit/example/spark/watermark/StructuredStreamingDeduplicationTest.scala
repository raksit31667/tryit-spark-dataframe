package raksit.example.spark.watermark

import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, HDFSCluster}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.TimestampType
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class StructuredStreamingDeduplicationTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfterEach {

  import spark.implicits._

  private var events: MemoryStream[Event] = _
  private var streamingSession: Dataset[Event] = _
  private var watermarkStreaming: StreamingQuery = _
  private var hdfsCluster: HDFSCluster = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    hdfsCluster = new HDFSCluster()
    hdfsCluster.startHDFS()
    events = MemoryStream[Event]
    streamingSession = events.toDS()
    watermarkStreaming = streamingSession.withColumn("enqueuedTime", from_unixtime($"time").cast(TimestampType))
      .withWatermark("enqueuedTime", "10 seconds")
      .dropDuplicates("data", "enqueuedTime")
      .writeStream
      .format("parquet")
      .option("checkpointLocation", hdfsCluster.getNameNodeURI() + "/checkpoints")
      .option("path", hdfsCluster.getNameNodeURI() + "/streaming")
      .outputMode(OutputMode.Append())
      .start()
  }

  override def afterEach(): Unit = {
    hdfsCluster.shutdownHDFS()
    super.afterAll()
  }

  test("should be streaming dataset when convert memory stream to event given memory stream") {
    // Then
    assert(streamingSession.isStreaming)
  }

  test("should return added event when processAllAvailable given new event with Thursday, 11 June 2020 03:58:21 UTC") {
    // Given
    events.addData(Event(1591847901L, "aaa"))

    // When
    watermarkStreaming.processAllAvailable()
    val parquetFileDF = spark.read.parquet(hdfsCluster.getNameNodeURI() + "/streaming")
    parquetFileDF.createOrReplaceTempView("testStreaming")
    val actual = spark.sql("select * from testStreaming")

    // Then
    val expected = Seq((1591847901L, "aaa", Timestamp.valueOf("2020-06-11 10:58:21"))).toDF("time", "data", "enqueuedTime")
    assertDataFrameDataEquals(expected, actual)
  }
}
