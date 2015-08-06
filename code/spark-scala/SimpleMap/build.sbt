name := "dataflow-scala"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "latest.release" % "test",
  "org.apache.spark" %% "spark-core" % "1.4.1",
  "com.github.scopt" %% "scopt" % "3.3.0"
)
