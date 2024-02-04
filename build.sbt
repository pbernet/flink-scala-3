name := "flink-scala-3"

version := "0.1"

scalaVersion := "3.3.1"

val flinkVersion = "1.17.2"

resolvers += Resolver.mavenLocal
resolvers += "apache.snapshots" at "https://repository.apache.org/content/repositories/snapshots"

javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies += "org.apache.flink" % "flink-streaming-java" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-clients" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-planner-loader" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-common" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-api-java" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-runtime" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-runtime-web" % flinkVersion

libraryDependencies += "org.apache.flink" % "flink-json" % flinkVersion

libraryDependencies += "io.circe" %% "circe-core" % "0.14.6"
libraryDependencies += "io.circe" %% "circe-generic" % "0.14.6"
libraryDependencies += "io.circe" %% "circe-parser" % "0.14.6"

// For FraudDetectionJob Example
// https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/try-flink/datastream/
libraryDependencies += "org.apache.flink" % "flink-walkthrough-common" % flinkVersion
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.4.12"


scalacOptions += "-deprecation"
scalacOptions += "-feature"

run / fork := true