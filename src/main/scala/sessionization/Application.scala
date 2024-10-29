package sessionization

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Application {

  private val SESSION_EXPIRED_TIME: Int = 30 * 60
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

  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName("Sessionization")
      .master("local[*]")
      .getOrCreate()

    val df = session.read
      .option("header", value = true)
      .schema(SCHEMA)
      .csv("../samples/2019-Oct.csv")
      .withColumn(
        "event_time",
        to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'")
      )

    val dfWithSessionId = augmentSessionId(df)

    session.stop()
  }

  def augmentSessionId(
      df: Dataset[Row]
  ): Dataset[Row] = {
    val windowSpec = Window.partitionBy(col("user_id")).orderBy("event_time")

    val dfWithTimeDiff = df
      .withColumn(
        "ts_diff",
        unix_timestamp(col("event_time")) - unix_timestamp(
          lag(col("event_time"), 1).over(windowSpec)
        )
      )
      .withColumn(
        "ts_diff",
        when(
          row_number().over(windowSpec) === 1 || col(
            "ts_diff"
          ) > SESSION_EXPIRED_TIME,
          -1
        ).otherwise(col("ts_diff"))
      )

    val dfWithSessionId = dfWithTimeDiff
      .withColumn(
        "session_id",
        when(col("ts_diff") === -1, uuid()).otherwise(null)
      )
      .withColumn(
        "session_id",
        last("session_id", ignoreNulls = true)
          .over(windowSpec.rowsBetween(Window.unboundedPreceding, 0))
      )
      .drop("ts_diff")

    dfWithSessionId
  }

}
