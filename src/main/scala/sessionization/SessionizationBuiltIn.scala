package sessionization

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

object SessionizationBuiltIn {

  private val SESSION_EXPIRED_TIME: Int = 30 * 60
  private val SHA_256 = 256

  private val session: SparkSession = SparkSession
    .builder()
    .appName("SessionizationBuiltIn")
    .master("local[*]")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()

  import session.implicits._

  def main(args: Array[String]): Unit = {
    // e.g. 2019-10-01
    val PROCESS_DATE = sys.env("date")
    // e.g. 00
    val PROCESS_HOUR = sys.env("hour")

    val df = session.read
      .schema(Encoders.product[BehaviorSchema].schema)
      .parquet("../behaviors")
      .filter($"date_hour" === f"${PROCESS_DATE}T${PROCESS_HOUR}Z")

    val sessionDf = augmentSessionId(df)

    sessionDf.write
      .partitionBy("date_hour")
      .mode("overwrite")
      .format("parquet")
      .save("../sessions")

    session.stop()
  }

  def augmentSessionId(dataset: Dataset[Row]): Dataset[Row] = {
    val windowSpec = Window.partitionBy("user_id").orderBy("event_timestamp")
    val eventTimeDiff = unix_timestamp($"event_timestamp") - unix_timestamp(
      lag($"event_timestamp", 1).over(windowSpec)
    )
    val sessionIdentify =
      sha2(concat_ws("-", $"user_id", $"event_timestamp"), SHA_256)

    // 1. lag 통해 time_diff 컬럼에 현재 event_timestamp, 이전 event_timestamp 차이를 저장
    // 2. time_diff 가 SESSION_EXPIRED_TIME 초과시 NULL, 첫 시작 event_timestamp 또한 NULL 로 저장
    val dfWithTimeDiff = dataset
      .withColumn(
        "event_timestamp",
        to_timestamp($"event_time", "yyyy-MM-dd HH:mm:ss 'UTC'")
      )
      .withColumn("time_diff", eventTimeDiff)
      .withColumn(
        "time_diff",
        when($"time_diff" > SESSION_EXPIRED_TIME, null)
          .otherwise($"time_diff")
      )

    // 1. 첫 시작 혹은 SESSION_EXPIRED_TIME 초과 ("time_diff".isNull) 일 때, sha2 통해 session_id 를 생성
    // 2. 나머지의 경우 NULL 저장한 후, last window 통하여 이전의 session_id를 연속되게 부여
    val dfWithSessionId = dfWithTimeDiff
      .withColumn(
        "session_id",
        when($"time_diff".isNull, sessionIdentify).otherwise(null)
      )
      .withColumn(
        "session_id",
        last("session_id", ignoreNulls = true)
          .over(windowSpec.rowsBetween(Window.unboundedPreceding, 0))
      )
      .drop("event_timestamp")
      .drop("time_diff")

    dfWithSessionId
  }

}
