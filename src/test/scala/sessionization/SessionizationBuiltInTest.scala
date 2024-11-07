package sessionization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.scalatest.flatspec.AnyFlatSpec
import sessionization.SessionizationBuiltIn.{augmentSessionId, loadPrevActiveSessions}

import java.sql.Timestamp

class SessionizationBuiltInTest extends AnyFlatSpec {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("SessionizationTest")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  "SessionId" should "generate unique session IDs for time gaps over SESSION_EXPIRED_TIME" in {
    // given
    val data = Seq(
      (
        "1",
        Timestamp.valueOf("2019-10-01 10:00:00"),
        "click",
        "prod1",
        "cat1",
        "code1",
        "brand1",
        10.0
      ),
      (
        "1",
        Timestamp.valueOf("2019-10-01 10:15:00"),
        "click",
        "prod2",
        "cat1",
        "code1",
        "brand2",
        12.0
      ),
      (
        "1",
        Timestamp.valueOf("2019-10-01 11:00:00"),
        "purchase",
        "prod3",
        "cat2",
        "code2",
        "brand1",
        15.0
      ),
      (
        "1",
        Timestamp.valueOf("2019-10-01 11:10:00"),
        "click",
        "prod4",
        "cat2",
        "code2",
        "brand2",
        14.0
      ),
      (
        "2",
        Timestamp.valueOf("2019-10-01 10:00:00"),
        "click",
        "prod5",
        "cat3",
        "code3",
        "brand3",
        20.0
      ),
      (
        "2",
        Timestamp.valueOf("2019-10-01 10:20:00"),
        "click",
        "prod6",
        "cat3",
        "code3",
        "brand1",
        22.0
      ),
      (
        "2",
        Timestamp.valueOf("2019-10-01 11:30:00"),
        "purchase",
        "prod7",
        "cat4",
        "code4",
        "brand3",
        25.0
      )
    ).toDF(
      "user_id",
      "event_time",
      "event_type",
      "product_id",
      "category_id",
      "category_code",
      "brand",
      "price"
    ).withColumn(
      "event_time",
      to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")
    )

    // when
    val result = augmentSessionId(data).collect()

    // then
    assert(result(0).getString(8) == result(1).getString(8))
    assert(result(1).getString(8) != result(2).getString(8))
    assert(result(2).getString(8) == result(3).getString(8))
    assert(result(4).getString(8) == result(5).getString(8))
    assert(result(5).getString(8) != result(6).getString(8))
  }

  "UnorderedData" should "generate correct session id" in {
    // given
    val data = Seq(
      (
        "1",
        Timestamp.valueOf("2019-10-01 10:00:00"),
        "click",
        "prod1",
        "cat1",
        "code1",
        "brand1",
        10.0
      ),
      (
        "1",
        Timestamp.valueOf("2019-10-01 11:00:00"),
        "purchase",
        "prod3",
        "cat2",
        "code2",
        "brand1",
        15.0
      ),
      (
        "1",
        Timestamp.valueOf("2019-10-01 10:15:00"),
        "click",
        "prod2",
        "cat1",
        "code1",
        "brand2",
        12.0
      )
    ).toDF(
      "user_id",
      "event_time",
      "event_type",
      "product_id",
      "category_id",
      "category_code",
      "brand",
      "price"
    ).withColumn(
      "event_time",
      to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")
    )

    // when
    val result = augmentSessionId(data).collect()

    // then
    assert(result(0).getString(8) == result(1).getString(8))
    assert(result(1).getString(8) != result(2).getString(8))
  }

  "loadPrevActiveSessions" should "load sessions that are only active and take the last session only" in {
    // given
    val data = Seq(
      SessionSchema(
        "2023-10-15 00:29:00 UTC",
        "click",
        1,
        101,
        "electronics.smartphone",
        "BrandA",
        299.99,
        1001,
        "session1"
      ),
      SessionSchema(
        "2023-10-15 00:40:00 UTC",
        "click",
        2,
        102,
        "electronics.tablet",
        "BrandB",
        399.99,
        1002,
        "session2"
      ),
      SessionSchema(
        "2023-10-15 00:45:00 UTC",
        "click",
        2,
        102,
        "electronics.tablet",
        "BrandB",
        399.99,
        1002,
        "session2"
      ),
      SessionSchema(
        "2023-10-15 00:50:00 UTC",
        "click",
        2,
        102,
        "electronics.tablet",
        "BrandB",
        399.99,
        1003,
        "session3"
      )
    ).toDF()
    val expected = Seq(
      SessionSchema(
        "2023-10-15 00:45:00 UTC",
        "click",
        2,
        102,
        "electronics.tablet",
        "BrandB",
        399.99,
        1002,
        "session2"
      ),
      SessionSchema(
        "2023-10-15 00:50:00 UTC",
        "click",
        2,
        102,
        "electronics.tablet",
        "BrandB",
        399.99,
        1003,
        "session3"
      )
    ).toDF()
    val processTime = "2023-10-15T01Z"

    // when
    val result = loadPrevActiveSessions(data, processTime)

    // then
    assert(result.collect() sameElements expected.collect())
  }
}
