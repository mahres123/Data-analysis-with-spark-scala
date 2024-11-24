ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "spark example",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1", // Spark Core
      "org.apache.spark" %% "spark-sql" % "3.5.1",  // Spark SQL

    )
  )
