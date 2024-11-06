package sessionization

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{date_format, to_timestamp}

object DailyFileDivider {

  // e.g. 2019-Oct
  private val YEAR_MONTH = sys.env("date")
  private val INPUT_CSV = f"../samples/$YEAR_MONTH.csv"

  private val session = SparkSession
    .builder()
    .appName("Sessionization")
    .master("local[*]")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()

  import session.implicits._

  def main(args: Array[String]): Unit = {
    val df = session.read
      .option("header", value = true)
      .schema(Encoders.product[BehaviorSchema].schema)
      .csv(INPUT_CSV)

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
      .save("../behaviors")

    session.stop()
  }
}
