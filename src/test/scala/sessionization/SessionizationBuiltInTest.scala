package sessionization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.flatspec.AnyFlatSpec
import sessionization.SessionizationBuiltIn.{
  augmentSessionId,
  loadPrevActiveSessions
}

import java.time.LocalDateTime

class SessionizationBuiltInTest extends AnyFlatSpec {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("SessionizationTest")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  "SessionId" should "generate unique session IDs for time gaps over SESSION_EXPIRED_TIME" in {
    // given
    val eventDateTime = LocalDateTime.parse("2019-10-01T10:00:00")
    val data = Seq(
      SessionSchema(
        "2019-10-01 10:00:00 UTC",
        "click",
        1,
        1,
        "code1",
        "brand1",
        10.0,
        1,
        null,
        "2019-10-01",
        "10"
      ),
      SessionSchema(
        "2019-10-01 10:10:00 UTC",
        "click",
        2,
        1,
        "code1",
        "brand2",
        12.0,
        1,
        null,
        "2019-10-01",
        "10"
      ),
      SessionSchema(
        "2019-10-01 10:41:00 UTC",
        "purchase",
        3,
        2,
        "code2",
        "brand1",
        15.0,
        1,
        null,
        "2019-10-01",
        "10"
      ),
      SessionSchema(
        "2019-10-01 10:50:00 UTC",
        "click",
        4,
        2,
        "code2",
        "brand2",
        14.0,
        1,
        null,
        "2019-10-01",
        "10"
      ),
      SessionSchema(
        "2019-10-01 10:00:00 UTC",
        "click",
        5,
        3,
        "code3",
        "brand3",
        20.0,
        2,
        null,
        "2019-10-01",
        "10"
      ),
      SessionSchema(
        "2019-10-01 10:20:00 UTC",
        "click",
        6,
        3,
        "code3",
        "brand1",
        22.0,
        2,
        null,
        "2019-10-01",
        "10"
      ),
      SessionSchema(
        "2019-10-01 10:55:00 UTC",
        "purchase",
        7,
        4,
        "code4",
        "brand3",
        25.0,
        2,
        null,
        "2019-10-01",
        "10"
      )
    ).toDF()

    // when
    val result = augmentSessionId(data, eventDateTime).collect()

    // then
    assert(result(0).getString(8) == result(2).getString(8))
    assert(result(1).getString(8) == result(3).getString(8))
    assert(result(4).getString(8) == result(5).getString(8))
    assert(result(2).getString(8) != result(4).getString(8))
    assert(result(3).getString(8) != result(6).getString(8))
  }

  "UnorderedData" should "generate correct session id" in {
    // given
    val eventDateTime = LocalDateTime.parse("2019-10-01T10:00:00")
    val data = Seq(
      SessionSchema(
        "2019-10-01 10:00:00 UTC",
        "click",
        1,
        1,
        "code1",
        "brand1",
        10.0,
        1,
        null,
        "2019-10-01",
        "10"
      ),
      SessionSchema(
        "2019-10-01 10:55:00 UTC",
        "purchase",
        3,
        2,
        "code2",
        "brand1",
        15.0,
        1,
        null,
        "2019-10-01",
        "10"
      ),
      SessionSchema(
        "2019-10-01 10:15:00 UTC",
        "click",
        2,
        1,
        "code1",
        "brand2",
        12.0,
        1,
        null,
        "2019-10-01",
        "10"
      )
    ).toDF()
      .withColumn("date_hour", lit(eventDateTime))

    // when
    val result = augmentSessionId(data, eventDateTime).collect()

    // then
    assert(result(0).getString(8) == result(1).getString(8))
    assert(result(1).getString(8) != result(2).getString(8))
  }

  "augmentSessionId" should "generate a correct session ID with PrevActiveSession" in {
    // given
    val eventDateTime = LocalDateTime.parse("2019-10-01T10:00:00")
    val prevActiveSession = Seq(
      SessionSchema(
        "2019-10-01 09:35:00 UTC",
        "click",
        1,
        1,
        "code1",
        "brand1",
        10.0,
        1,
        "session1",
        "2019-10-01",
        "09"
      ),
      SessionSchema(
        "2019-10-01 09:45:00 UTC",
        "click",
        1,
        1,
        "code1",
        "brand1",
        10.0,
        2,
        "session2",
        "2019-10-01",
        "09"
      )
    ).toDF()
    val behaviorData = Seq(
      BehaviorSchema(
        "2019-10-01 10:06:00 UTC",
        "click",
        1,
        1,
        "code1",
        "brand1",
        10.0,
        1,
        "2019-10-01",
        "10"
      ),
      BehaviorSchema(
        "2019-10-01 10:10:00 UTC",
        "click",
        2,
        1,
        "code1",
        "brand2",
        12.0,
        2,
        "2019-10-01",
        "10"
      ),
      BehaviorSchema(
        "2019-10-01 10:20:00 UTC",
        "purchase",
        3,
        2,
        "code2",
        "brand1",
        15.0,
        1,
        "2019-10-01",
        "10"
      )
    ).toDF()
      .withColumn("date_hour", lit(eventDateTime))
    val data =
      prevActiveSession.unionByName(behaviorData, allowMissingColumns = true)

    // when
    val result = augmentSessionId(data, eventDateTime).collect()

    assert(result(0).getString(8) != "session1")
    assert(result(0).getString(8) == result(2).getString(8))
    assert(result(1).getString(8) == "session2")
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
        "session1",
        "2023-10-15",
        "00"
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
        "session2",
        "2023-10-15",
        "00"
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
        "session2",
        "2023-10-15",
        "00"
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
        "session3",
        "2023-10-15",
        "00"
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
        "session2",
        "2023-10-15",
        "00"
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
        "session3",
        "2023-10-15",
        "00"
      )
    ).toDF()
    val eventDateTime = LocalDateTime.parse("2023-10-15T01:00:00")

    // when
    val result = loadPrevActiveSessions(data, eventDateTime)

    // then
    assert(result.collect() sameElements expected.collect())
  }
}
