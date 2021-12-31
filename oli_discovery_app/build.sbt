import Dependencies._

ThisBuild / scalaVersion     := "2.12.10"
ThisBuild / version          := "1.0"
ThisBuild / organization     := "com.btaj"
ThisBuild / organizationName := "btaj"


lazy val root = (project in file("."))
  .settings(
    name := "OliDiscovery",
    libraryDependencies += scalaCtic,
    libraryDependencies += scalaTest,
    libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "3.0.1_1.0.0" % Test,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1",
    libraryDependencies += sparkCore,
    libraryDependencies += sparkMLlib
  )

scalacOptions ++= Seq(
  "-encoding", "utf8", // Option and arguments on same line
  "-Xfatal-warnings",  // New lines for each options
  "-deprecation",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps"
)
javacOptions ++= Seq("-encoding", "utf8")
//override def mainClass = T { Some("SparkCli") }

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
//assemblyJarName in assembly := "OliDiscovery-assembly-1.0.jar"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

// add plugins coverage
//coverageEnabled := true


// create assembly.sbt in project folder then add addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "1.0.0")
// back to app folder launch sbt clean+compile+reload  then sbt assembly
// jar is created in app\target\scala-2.12\OliDiscovery-assembly-1.0.jar
// spark-submit --class SparkCli target/scala-2.12/OliDiscovery-assembly-1.0.jar "option1" "data/olist_orders_dataset.csv" "output"
// spark-submit --class SparkCli target/scala-2.12/OliDiscovery-assembly-1.0.jar "report_option1" "data/olist_orders_dataset.csv" "output"
// spark-submit --class SparkCli target/scala-2.12/OliDiscovery-assembly-1.0.jar "option2" "data/olist_orders_dataset.csv" "output"
// spark-submit --class SparkCli target/scala-2.12/OliDiscovery-assembly-1.0.jar "report_option2" "data/olist_orders_dataset.csv" "output"
// create plugins.sbt in project folder then add addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.1")
// launch sbt then clean then compile then test then coverage then coverageReport
// clean coverage test coverageReport from scoverage