package sessionization

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}

object DailyFileDivider {

  private val session =
    SparkSession
      .builder()
      .appName("Sessionization")
      .master("yarn")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

  import session.implicits._

  def main(args: Array[String]): Unit = {
    val yearMonth = args(0)
    val df = session.read
      .option("header", value = true)
      .schema(Encoders.product[BehaviorSchema].schema)
      .csv(s"gs://daeuk/behaviors/raws/$yearMonth.csv")

    val hourlyDf = df.withColumn(
      "date_hour",
      date_format(
        to_timestamp($"event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"),
        "yyyy-MM-dd'T'HH'Z'"
      )
    )

    hourlyDf
      .repartition($"date_hour")
      .write
      .partitionBy("date_hour")
      .mode("overwrite")
      .format("parquet")
      .save("gs://daeuk/behaviors/logs")

    session.stop()
  }
}
