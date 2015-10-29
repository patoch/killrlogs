name := "killrlogs-spark-streaming"

version := "1.0"

scalaVersion := "2.10.5"


libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0" % "provided",
  "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.4.1" % "provided",
  ("org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.1").
    exclude("commons-beanutils", "commons-beanutils").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog")

)
mergeStrategy in assembly := {

  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class")
  => MergeStrategy.first
  case x => (mergeStrategy in assembly).value(x)

}

mainClass in assembly := Some("com.datastax.demo.killrlogs.KillrLogsStreamingApp")

assemblyJarName in assembly := "killrlogs-streaming.jar"