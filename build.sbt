name := "flink-scala-3"

version := "0.1"

scalaVersion := "3.1.3"

val flinkVersion = "1.15.1"

resolvers += Resolver.mavenLocal

javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies += "org.apache.flink" % "flink-streaming-java" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-clients" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-planner-loader" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-common" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-api-java" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-api-java-bridge" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-table-runtime" % flinkVersion
libraryDependencies += "org.apache.flink" % "flink-connector-kafka" % flinkVersion

libraryDependencies += "org.apache.flink" % "flink-json" % flinkVersion
// Works with Scala 3
libraryDependencies += "com.github.losizm" %% "little-json" % "9.0.0"

// For FraudDetectionJob Example
// https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/try-flink/datastream/#fraud-detection-with-the-datastream-api
libraryDependencies += "org.apache.flink" % "flink-walkthrough-common" % flinkVersion
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.11"
