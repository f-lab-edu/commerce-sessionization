# commerce-sessionization

### Environment

- Airflow
- DataProc

본인의 경우 `Airflow`, `DataProc` GCE 환경에서 진행하였습니다.

### Data Source / Load

- https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

`DailyFileDivider.scala` 파일 내 코드를 다음과 같이 변경해 데이터 적재

예시는 +1877일 데이터, 2019년 10월 01일 -> 2024년 11월 20일 데이터 변환 후 적재

```scala
val hourlyDf = df
  .withColumn(
    "event_time",
    from_unixtime(
      unix_timestamp(
        $"event_time",
        "yyyy-MM-dd HH:mm:ss 'UTC'"
      ) + (1877L * 24 * 60 * 60),
      "yyyy-MM-dd HH:mm:ss 'UTC'"
    )
  )
```

### Jar Compile

`build.sbt` 파일 내 다음과 같이 추가하여 mainClass 명시, `sbt assembly` 실행

```sbt
mainClass := Some("SessionizationBuiltIn")
assembly / assemblyJarName := "SessionizationBuiltIn.jar"
```
