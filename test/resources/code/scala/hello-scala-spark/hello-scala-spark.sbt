name := "hello-scala-spark"
version := "1.0"
scalaVersion := "2.12.12"
useCoursier := false
retrieveManaged := true
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.6.9"
mainClass in (Compile, packageBin) := Some("HelloScalaSparkApp")
mainClass in (Compile, run) := Some("HelloScalaSparkApp")
