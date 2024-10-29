package sessionization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.scalatest.flatspec.AnyFlatSpec
import sessionization.Application.augmentSessionId

class ApplicationTest extends AnyFlatSpec {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("SessionizationTest")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  "SessionId" should "generate unique session IDs for time gaps over SESSION_EXPIRED_TIME" in {
    // given
    val sample = Seq(
      ("1", "2019-10-01 10:00:00"),
      ("1", "2019-10-01 10:15:00"),
      ("1", "2019-10-01 11:00:00"), // 새로운 세션 시작 (30분 초과)
      ("1", "2019-10-01 11:10:00"),
      ("2", "2019-10-01 10:00:00"), // 새로운 user_id
      ("2", "2019-10-01 10:20:00"),
      ("2", "2019-10-01 11:30:00") // 새로운 세션 시작 (30분 초과)
    ).toDF("user_id", "event_time")
      .withColumn(
        "event_time",
        to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")
      )

    // when
    val result = augmentSessionId(sample).collect()

    // then
    assert(result(0).getString(2) == result(1).getString(2))
    assert(result(1).getString(2) != result(2).getString(2))
    assert(result(2).getString(2) == result(3).getString(2))
    assert(result(4).getString(2) == result(5).getString(2))
    assert(result(5).getString(2) != result(6).getString(2))
  }
}
