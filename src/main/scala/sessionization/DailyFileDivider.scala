package sessionization

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

object DailyFileDivider {

  private val session =
    SparkSession
      .builder()
      .appName("DailyFileDivider")
      .master("yarn")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

  import session.implicits._

  def main(args: Array[String]): Unit = {
    // e.g. 2019-Oct
    val yearMonth = args(0)
    val df = session.read
      .option("header", value = true)
      .schema(Encoders.product[BehaviorSchema].schema)
      .csv(s"gs://daeuk/behaviors/raws/$yearMonth.csv")

    val hourlyDf = df
      .withColumn(
        "event_date",
        date_format(
          to_timestamp($"event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"),
          "yyyy-MM-dd"
        )
      )
      .withColumn(
        "event_hour",
        date_format(
          to_timestamp($"event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"),
          "HH"
        )
      )

    hourlyDf
      .repartition($"event_date", $"event_hour")
      .write
      .partitionBy("event_date", "event_hour")
      .mode("overwrite")
      .format("parquet")
      .save("gs://daeuk/behaviors/logs")

    session.stop()
  }
}
