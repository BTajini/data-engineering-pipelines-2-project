import org.apache.spark.sql.SparkSession

object SparkCli {

    import org.apache.log4j.Logger

    def run(command: Array[String]) = command match {
        case Array("option1", csvfile, exportfile, testing) => OliBatch.run((spark: SparkSession) => OliBatch.option1(spark, csvfile, exportfile, testing))
        case Array("option2", csvfile, export_file, testing) => OliBatch.run((spark: SparkSession) => OliBatch.option2(spark, csvfile, export_file, testing))
        case Array("report_option1", csvfile, testing) => OliBatch.run((spark: SparkSession) => OliBatch.report_option1(spark, csvfile, testing))
        case Array("report_option2", csvfile, testing) => OliBatch.run((spark: SparkSession) => OliBatch.report_option2(spark, csvfile, testing))
        case _ => println("command - " + command.deep + " - not recognized, valid params : (option1|option2|report_option1|report_option2)")
    }

    def main(args: Array[String]) = {
        println("*** OliDiscovery Command-line choice : " + args(0) + " ***")
        if(args(0) == "option1" || args(0) == "option2"){
          run(Array(args(0), args(1), args(2), args(3)))
        } else { 
          run(Array(args(0), args(1), args(2)))
        }
        
        
    }

}


