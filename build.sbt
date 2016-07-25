name := "NCAA Tournament Prediction"
version := "0.1"
scalaVersion := "2.11.7"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-sql" % "1.5.2",
  "org.apache.spark" %% "spark-mllib" % "1.5.2",
  "com.databricks" %% "spark-csv" % "1.4.0"
)
