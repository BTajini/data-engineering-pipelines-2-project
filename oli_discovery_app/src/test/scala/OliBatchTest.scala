import com.holdenkarau.spark.testing._
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite




class OliBatchTest extends AnyFunSuite with DataFrameSuiteBase {

        test("Testing all options from OliBatch and SparkCli...") {
                
                val spark = SparkSession.builder().master("local[*]").appName("Oli Batch").getOrCreate()
                val csvfile = "data/olist_orders_dataset.csv"
                val exportfile = "output"
                val testing = "true"
        
                import spark.implicits._
                import org.apache.spark.sql._
                import org.apache.spark.sql.functions._

                println("************Testing option1...************")
                val option1_dataframe = OliBatch.option1(spark, csvfile, exportfile, testing) 
                assert(option1_dataframe.isEmpty === false)
                assert(option1_dataframe.filter(col("delivery_delay_in_days") <=10).count === 0)
                println("************Testing option1 done...************")

                println("************Testing option2...************")
                val option2_dataframe = OliBatch.option2(spark, csvfile, exportfile, testing)
                assert(option2_dataframe.isEmpty === false)
                assert(option2_dataframe.filter(col("delivery_delay_in_days") <=10).count === 0)
                println("************Testing option2 done...************")

                
                println("************Testing report_option1...************")
                val report_option1_dataframe = OliBatch.report_option1(spark, csvfile, testing) 
                assert(report_option1_dataframe.isEmpty === false)
                assert(report_option1_dataframe.filter(col("delivery_delay_in_days") <=10).count === 0)
                println("************Testing report_option1 done...************")

                println("************Testing report_option2...************")
                val report_option2 = OliBatch.report_option2(spark, csvfile, testing) 
                assert(report_option2.isEmpty === false)
                assert(report_option2.filter(col("delivery_delay_in_days") <=10).count === 0)
                println("************Testing report_option2 done...************")
                
                println("************Testing SparkCli_option1...************")
                val SparkCli_option1 = SparkCli.main(Array("option1", csvfile, exportfile, testing))
                println("SparkCli option1 is working...")
                println("************Testing SparkCli_option1 done...************")

                println("************Testing SparkCli_option2...************")
                val SparkCli_option2 = SparkCli.main(Array("option2", csvfile, exportfile, testing))
                println("SparkCli option2 is working...")
                println("************Testing SparkCli_option2 done...************")

                println("************Testing SparkCli_report_option1...************")
                val SparkCli_report_option1 = SparkCli.main(Array("report_option1", csvfile, testing))
                println("SparkCli report option1 is working...")
                println("************Testing SparkCli_report_option1 done...************")

                println("************Testing SparkCli_report_option2...************")
                val SparkCli_report_option2 = SparkCli.main(Array("report_option2", csvfile, testing))
                println("SparkCli option2 is working...")
                println("************Testing SparkCli_report_option2 done...************")

                println("************Testing SparkCli_nocommand...************")
                val SparkCli_nocommand = SparkCli.main(Array("option3", csvfile, exportfile, testing))
                println("SparkCli no command is working...")
                println("************Testing SparkCli_nocommand done...************")
                
                //println("Testing SparkCli done")
                // TODO adding assert for all those functions and testing condition in menu
                
        }
        
        test("Testing deliveries delayed for more than 10 days following option1 logic...") {
                
                val spark = SparkSession.builder().master("local[*]").appName("Oli Batch").getOrCreate()
                val csvfile = "data/olist_orders_dataset.csv"
                val exportfile = "output"
        
                import spark.implicits._
                import org.apache.spark.sql._
                import org.apache.spark.sql.functions._
                

                val ordersDF = spark.read.option("header", "true").csv(csvfile)
                def extractDeliveryMoreThan10Days(df: DataFrame)= {
                        
                        val ordersDF = df.withColumn("delivery_delay_in_days",
                                                datediff(col("order_delivered_customer_date"),
                                                col("order_purchase_timestamp")))
                        ordersDF.createOrReplaceTempView("ordersDF")
                        spark.sql("""SELECT order_id,
                        customer_id,
                        order_purchase_timestamp,
                        order_delivered_customer_date,
                        order_status,
                        delivery_delay_in_days 
                        FROM ordersDF where delivery_delay_in_days > 10 and order_status == 'delivered'""")
                        }

                // Sanity check, verify no deliveries beyond 10 days, should return true
                
                println("************Sanity check option1...************")
                val DfExported = extractDeliveryMoreThan10Days(ordersDF)
                println("Delayed deliveries beyond 10 days : ")
                assert(DfExported.isEmpty === false)
                assert(DfExported.filter(col("delivery_delay_in_days") <=10).count === 0)
                println(DfExported.filter(col("delivery_delay_in_days") <=10).count == 0)
                println("************Sanity check done option1...************")
                println("************Count rows option1...************")
                assert(DfExported.count() === 46604)
                println(DfExported.count() == 46604)  // rows = 46604
                println("************Count rows option1 done...************")
                        
        }
        

        test("Testing deliveries delayed for more than 10 days following option2 logic...") {
                
                val spark = SparkSession.builder().master("local[*]").appName("Oli Batch").getOrCreate()
                val csvfile = "data/olist_orders_dataset.csv"
                val exportfile = "output"
        
                import spark.implicits._
                import org.apache.spark.sql._
                import org.apache.spark.sql.functions._
                

                val ordersDF = spark.read.option("header", "true").csv(csvfile)
                def extractDeliveryMoreThan10Days(df: DataFrame)= {
                        
                        val ordersDF = df.select(col("order_id"),
                                            col("customer_id"),
                                            col("order_purchase_timestamp"),
                                            col("order_delivered_customer_date"),
                                            col("order_status"),
                                            lit("America/Sao_Paulo").as("timezone")).where("order_status == 'delivered'")
                                        .withColumn("order_purchase_timestamp_UTC", 
                                        to_utc_timestamp(col("order_purchase_timestamp"), col("timezone")))
                                        .withColumn("order_delivered_customer_date_UTC",
                                        to_utc_timestamp(col("order_delivered_customer_date"), col("timezone")))
                                        .drop("timezone")
                                        .drop("order_purchase_timestamp")
                                        .drop("order_delivered_customer_date")
                                        .withColumn("daylight_saving_time", when(col("order_delivered_customer_date_UTC")
                                        .between("2016-02-21 00:00:00","2016-10-16 00:00:00") ,"Winter_time")
                                        .when(col("order_delivered_customer_date_UTC")
                                        .between("2017-02-19 00:00:00","2017-10-15 00:00:00") ,"Winter_time")
                                        .when(col("order_delivered_customer_date_UTC")
                                        .between("2018-02-18 00:00:00","2018-11-04 00:00:00") ,"Winter_time")
                                        .otherwise("Summer_time"))
                                        .withColumn("delivery_delay_in_days",
                                        datediff(col("order_delivered_customer_date_UTC"),
                                        col("order_purchase_timestamp_UTC")))
                                        .withColumn("order_delivered_customer_date",date_format(col("order_delivered_customer_date_UTC"), "yyyy-MM-dd HH:mm:ss"))
                                        .withColumn("order_purchase_timestamp",date_format(col("order_purchase_timestamp_UTC"), "yyyy-MM-dd HH:mm:ss"))
                                        .drop("order_delivered_customer_date_UTC")
                                        .drop("order_purchase_timestamp_UTC")
                        ordersDF.createOrReplaceTempView("ordersDF")
                        spark.sql("""SELECT order_id,
                        customer_id,
                        order_purchase_timestamp,
                        order_delivered_customer_date,
                        order_status,
                        daylight_saving_time,
                        delivery_delay_in_days 
                        FROM ordersDF where delivery_delay_in_days > 10
                        """)
                        }

                // Sanity check, verify no deliveries beyond 10 days, should return true
                
                println("************Sanity check option2...************")
                val DfExported = extractDeliveryMoreThan10Days(ordersDF)
                println("Delayed deliveries beyond 10 days : ")
                assert(DfExported.isEmpty === false)
                assert(DfExported.filter(col("delivery_delay_in_days") <=10).count === 0)
                println(DfExported.filter(col("delivery_delay_in_days") <=10).count == 0)
                println("************Sanity check option2 done...************")
                println("************Count rows option2...************")
                assert(DfExported.count() === 46722)
                println(DfExported.count() == 46722)  // rows = 46722
                println("************Count rows done option2...************")
                        
        }
        
}