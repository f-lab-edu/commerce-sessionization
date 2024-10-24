lazy val buildSettings = Seq(
  name := "commerce-sessionization",
  version := "0.1",
  scalaVersion := "2.13.15"
)

lazy val app = (project in file(".")).
  settings(buildSettings)

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
