package sessionization

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object SessionizationBuiltIn {

  private val SESSION_EXPIRED_TIME: Int = 30 * 60
  private val SHA_256 = 256
  private val DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val HOUR_FORMAT = DateTimeFormatter.ofPattern("HH")

  private val session: SparkSession = SparkSession
    .builder()
    .appName("Sessionization")
    .master("yarn")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()

  import session.implicits._

  def main(args: Array[String]): Unit = {
    // e.g. 2019-10-01
    val eventDate = args(0)
    // e.g. 00
    val eventHour = args(1)
    val eventDateTime = LocalDateTime.parse(
      s"$eventDate $eventHour",
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH")
    )

    val behaviorData = session.read
      .schema(Encoders.product[BehaviorSchema].schema)
      .parquet("gs://daeuk/behaviors/logs")
      .filter($"event_date" === eventDate && $"event_hour" === eventHour)

    val prevSessionData = session.read
      .schema(Encoders.product[SessionSchema].schema)
      .parquet("gs://daeuk/behaviors/sessions")
      .filter(
        $"event_date" === eventDateTime.minusHours(1L).format(DATE_FORMAT)
          && $"event_hour" === eventDateTime.minusHours(1L).format(HOUR_FORMAT)
      )

    val unionData = loadPrevActiveSessions(prevSessionData, eventDateTime)
      .unionByName(behaviorData, allowMissingColumns = true)

    val sessionDf = augmentSessionId(unionData, eventDateTime)

    sessionDf.show(false)

    sessionDf.write
      .partitionBy("event_date", "event_hour")
      .mode("overwrite")
      .format("parquet")
      .save("gs://daeuk/behaviors/sessions")

    session.stop()
  }

  def augmentSessionId(
      dataset: Dataset[Row],
      eventDateTime: LocalDateTime
  ): Dataset[Row] = {
    val windowSpec = Window.partitionBy("user_id").orderBy("event_timestamp")
    val eventTimeDiff = unix_timestamp($"event_timestamp") - unix_timestamp(
      lag($"event_timestamp", 1).over(windowSpec)
    )
    val assignSessionId =
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

    // 이전 Active Session 일 때 session_id 그대로
    // 세션의 첫 시작 혹은 SESSION_EXPIRED_TIME 초과일 때 sha256 통해 assignSessionId
    // 나머지는 NULL 저장 이후 last window 통하여 이전의 session_id 부여
    // 시간 기준 정렬 이후 반환
    dfWithTimeDiff
      .withColumn(
        "session_id",
        when($"session_id".isNotNull, $"session_id")
          .when($"time_diff".isNull, assignSessionId)
          .otherwise(null)
      )
      .withColumn(
        "session_id",
        last("session_id", ignoreNulls = true)
          .over(windowSpec.rowsBetween(Window.unboundedPreceding, 0))
      )
      .filter($"event_hour" === eventDateTime.format(HOUR_FORMAT))
      .sort("event_timestamp")
      .drop("event_timestamp")
      .drop("time_diff")
  }

  // 1. eventDateTime 이전 30분 이내 행만 필터링
  // 2. session_id 기준 파티션과 event_timestamp 내림차순으로 row_number() 사용하여 가장 최근 세션 선택
  def loadPrevActiveSessions(
      dataset: Dataset[Row],
      eventDateTime: LocalDateTime
  ): Dataset[Row] = {
    val windowSpec =
      Window.partitionBy("session_id").orderBy(desc("event_timestamp"))

    dataset
      .withColumn(
        "event_timestamp",
        to_timestamp($"event_time", "yyyy-MM-dd HH:mm:ss 'UTC'")
      )
      .filter(before30Minutes(eventDateTime) <= $"event_timestamp")
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" === 1)
      .drop("rank")
      .drop("event_timestamp")
  }

  private def before30Minutes(eventDateTime: LocalDateTime): Column = {
    val formattedDateTime = eventDateTime
      .minusMinutes(30)
      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    to_timestamp(lit(formattedDateTime))
  }

}
