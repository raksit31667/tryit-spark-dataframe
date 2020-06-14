package raksit.example.spark.watermark

import java.sql.Timestamp

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, HDFSCluster}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.TimestampType
import org.scalatest.FunSuite

class StructuredStreamingDeduplicationTest extends FunSuite with DataFrameSuiteBase {

  import spark.implicits._

  private var events: MemoryStream[Event] = _
  private var streamingSession: Dataset[Event] = _
  private var watermarkStreaming: StreamingQuery = _
  private var hdfsCluster: HDFSCluster = _

  override def beforeAll(): Unit = {
    super.beforeAll()
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

  override def afterAll(): Unit = {
    hdfsCluster.shutdownHDFS()
    super.afterAll()
  }

  test("should be streaming dataset when convert memory stream to event given memory stream") {
    // Then
    assert(streamingSession.isStreaming)
  }

  test("should return added event when processAllAvailable given new event with Thursday, 11 June 2020 03:58:21 UTC") {
    // When
    events.addData(Event(1591847901L, "aaa"))
    watermarkStreaming.processAllAvailable()
    val parquetFileDF = spark.read.parquet(hdfsCluster.getNameNodeURI() + "/streaming")
    parquetFileDF.createOrReplaceTempView("testStreaming")
    val actual = spark.sql("select * from testStreaming")

    // Then
    val expected = Seq((1591847901L, "aaa", Timestamp.valueOf("2020-06-11 10:58:21"))).toDF("time", "data", "enqueuedTime")
    assertDataFrameDataEquals(expected, actual)
  }

  test("should return 2 events when processAllAvailable given new event with Thursday, 11 June 2020 03:58:26 UTC") {
    // Given
    events.addData(Event(1591847901L, "aaa"))
    watermarkStreaming.processAllAvailable()

    // When
    events.addData(Event(1591847901L + 5, "aaa"))
    watermarkStreaming.processAllAvailable()
    val parquetFileDF = spark.read.parquet(hdfsCluster.getNameNodeURI() + "/streaming")
    parquetFileDF.createOrReplaceTempView("testStreaming")
    val actual = spark.sql("select * from testStreaming")

    // Then
    val expected = Seq(
      (1591847901L, "aaa", Timestamp.valueOf("2020-06-11 10:58:21")),
      (1591847901L + 5, "bbb", Timestamp.valueOf("2020-06-11 10:58:26"))
    ).toDF("time", "data", "enqueuedTime")

    assertDataFrameDataEquals(expected, actual)
  }

  test("should return 4 events when processAllAvailable given new event with Thursday, 11 June 2020 03:58:31 and 03:58:36 UTC") {
    // Given
    events.addData(Event(1591847901L, "aaa"))
    watermarkStreaming.processAllAvailable()
    events.addData(Event(1591847901L + 5, "aaa"))
    watermarkStreaming.processAllAvailable()

    // When
    events.addData(Event(1591847901L + 10, "ccc"))
    events.addData(Event(1591847901L + 15, "ddd"))
    watermarkStreaming.processAllAvailable()
    val parquetFileDF = spark.read.parquet(hdfsCluster.getNameNodeURI() + "/streaming")
    parquetFileDF.createOrReplaceTempView("testStreaming")
    val actual = spark.sql("select * from testStreaming")

    // Then
    val expected = Seq(
      (1591847901L, "aaa", Timestamp.valueOf("2020-06-11 10:58:21")),
      (1591847901L + 5, "bbb", Timestamp.valueOf("2020-06-11 10:58:26")),
      (1591847901L + 10, "ccc", Timestamp.valueOf("2020-06-11 10:58:31")),
      (1591847901L + 15, "ddd", Timestamp.valueOf("2020-06-11 10:58:36"))
    ).toDF("time", "data", "enqueuedTime")

    assertDataFrameDataEquals(expected, actual)
  }

  test("should return 4 events when processAllAvailable given duplicated event with Thursday, 11 June 2020 03:58:21 UTC") {
    // Given
    events.addData(Event(1591847901L, "aaa"))
    watermarkStreaming.processAllAvailable()
    events.addData(Event(1591847901L + 5, "aaa"))
    watermarkStreaming.processAllAvailable()
    events.addData(Event(1591847901L + 10, "ccc"))
    events.addData(Event(1591847901L + 15, "ddd"))
    watermarkStreaming.processAllAvailable()

    // When
    events.addData(Event(1591847901L, "aaa"))
    watermarkStreaming.processAllAvailable()
    val parquetFileDF = spark.read.parquet(hdfsCluster.getNameNodeURI() + "/streaming")
    parquetFileDF.createOrReplaceTempView("testStreaming")
    val actual = spark.sql("select * from testStreaming")

    // Then
    val expected = Seq(
      (1591847901L, "aaa", Timestamp.valueOf("2020-06-11 10:58:21")),
      (1591847901L + 5, "bbb", Timestamp.valueOf("2020-06-11 10:58:26")),
      (1591847901L + 10, "ccc", Timestamp.valueOf("2020-06-11 10:58:31")),
      (1591847901L + 15, "ddd", Timestamp.valueOf("2020-06-11 10:58:36"))
    ).toDF("time", "data", "enqueuedTime")

    assertDataFrameDataEquals(expected, actual)
  }

  test("should return 4 events when processAllAvailable given late event with Thursday, 11 June 2020 03:58:11 UTC") {
    // Given
    events.addData(Event(1591847901L, "aaa"))
    watermarkStreaming.processAllAvailable()
    events.addData(Event(1591847901L + 5, "aaa"))
    watermarkStreaming.processAllAvailable()
    events.addData(Event(1591847901L + 10, "ccc"))
    events.addData(Event(1591847901L + 15, "ddd"))
    watermarkStreaming.processAllAvailable()

    // When
    events.addData(Event(1591847901L - 10, "eee"))
    watermarkStreaming.processAllAvailable()
    val parquetFileDF = spark.read.parquet(hdfsCluster.getNameNodeURI() + "/streaming")
    parquetFileDF.createOrReplaceTempView("testStreaming")
    val actual = spark.sql("select * from testStreaming")

    // Then
    val expected = Seq(
      (1591847901L, "aaa", Timestamp.valueOf("2020-06-11 10:58:21")),
      (1591847901L + 5, "bbb", Timestamp.valueOf("2020-06-11 10:58:26")),
      (1591847901L + 10, "ccc", Timestamp.valueOf("2020-06-11 10:58:31")),
      (1591847901L + 15, "ddd", Timestamp.valueOf("2020-06-11 10:58:36"))
    ).toDF("time", "data", "enqueuedTime")

    assertDataFrameDataEquals(expected, actual)
  }
}
