import sbt._


object Dependencies {
    lazy val scalaCtic = "org.scalactic" %% "scalactic" % "3.2.10"
    lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.10" % "test"
    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
    lazy val sparkCore = "org.apache.spark" %% "spark-core" % "2.4.0"
    // https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
    lazy val sparkMLlib = "org.apache.spark" %% "spark-mllib" % "2.4.0"
}

