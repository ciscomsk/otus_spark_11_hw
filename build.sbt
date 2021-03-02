name := "spark_custom_connector"

version := "0.1"

scalaVersion := "2.12.13"

val testcontainersScalaVersion = "0.39.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.2",
  "org.postgresql" % "postgresql" % "42.2.19",
  "org.scalatest" %% "scalatest" % "3.2.5" % "test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersScalaVersion % "test",
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersScalaVersion % "test",
)

