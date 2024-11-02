package sessionization

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.Timestamp
import java.util.UUID

object Sessionization {
  private val SESSION_EXPIRED_TIME: Long = 30 * 60 * 1000L
  private val SCHEMA: StructType = StructType(
    Seq(
      StructField("event_time", StringType),
      StructField("event_type", StringType),
      StructField("product_id", LongType),
      StructField("category_id", LongType),
      StructField("category_code", StringType),
      StructField("brand", StringType),
      StructField("price", DoubleType),
      StructField("user_id", LongType)
    )
  )
  private val session = SparkSession
    .builder()
    .appName("Sessionization")
    .master("local[*]")
    .getOrCreate()

  import session.implicits._

  def main(args: Array[String]): Unit = {
    val df = session.read
      .option("header", value = true)
      .schema(SCHEMA)
      .csv("../samples/2019-Oct.csv")
      .withColumn(
        "event_time",
        to_timestamp($"event_time", "yyyy-MM-dd HH:mm:ss 'UTC'")
      )

    augmentSessionId(df)

    session.stop()
  }

  def augmentSessionId(df: Dataset[Row]): Dataset[Row] = {
    val events = struct(
      $"event_time",
      $"event_type",
      $"product_id",
      $"category_id",
      $"category_code",
      $"brand",
      $"price"
    )

    // UDF: 세션화 로직을 수행하여 세션 ID(uuid)를 할당
    val assignSessionId = udf((event_times: Seq[Timestamp]) => {
      val headSessionIds = Seq[String](UUID.randomUUID().toString)
      val headTime = event_times.head

      // 각 이벤트 시간에 대해 sessionId 할당, SESSION_EXPIRED_TIME 초과 시 새로운 sessionId 생성
      event_times.tail
        .foldLeft((headSessionIds, headTime)) {
          case ((sessionIds, prevTime), currentTime) =>
            val sessionId =
              if (currentTime.getTime - prevTime.getTime > SESSION_EXPIRED_TIME)
                UUID.randomUUID().toString
              else
                sessionIds.last
            (sessionIds :+ sessionId, currentTime)
        }
        ._1
    })

    /* 각 사용자별로 그룹화하고 세션 ID 할당
     * 1. user_id 으로 Group By, 다른 모든 컬럼들을 collect_list 및 정렬 => events
     * 2. UDF(assignSessionId) 통하여 event_times 를 기준으로 session id 할당 => session_ids
     * 3. events 와 session_ids 를 zip 이후 explode 하여 원하는 결과 값 반환
     */
    df.groupBy("user_id")
      .agg(array_sort(collect_list(events)).as("events"))
      .withColumn("event_times", $"events.event_time")
      .withColumn("session_ids", assignSessionId($"event_times"))
      .withColumn(
        "events_with_session",
        zip_with(
          $"events",
          $"session_ids",
          (e, s) =>
            struct(
              e.getField("event_time").as("event_time"),
              e.getField("event_type").as("event_type"),
              e.getField("product_id").as("product_id"),
              e.getField("category_id").as("category_id"),
              e.getField("category_code").as("category_code"),
              e.getField("brand").as("brand"),
              e.getField("price").as("price"),
              s.as("session_id")
            )
        )
      )
      .select(
        $"user_id",
        explode($"events_with_session").as("event_with_session")
      )
      .select($"user_id", $"event_with_session.*")
  }

}
