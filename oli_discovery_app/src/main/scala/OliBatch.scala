import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import scala.reflect.io.Directory
import java.io.File
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level




object OliBatch {
    val log = Logger.getLogger(OliBatch.getClass().getName())
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    def run(f: SparkSession => Unit) = {
        val builder = SparkSession.builder.appName("Oli Batch").config("spark.master", "local")
        val spark = builder.getOrCreate()
        f(spark)
        spark.close
    }


        

    def option1(spark: SparkSession, csvfile: String, exportfile: String, testing: String): DataFrame = {
       import spark.implicits._
       import org.apache.spark.sql._

       def read_csvfile = spark.read.option("header", true).csv(csvfile)

       def orders_delayed_deliveries_10Days(df: DataFrame) = {
            val orders_original = df.select(col("order_id"),
                                            col("customer_id"),
                                            col("order_purchase_timestamp"),
                                            col("order_delivered_customer_date"),
                                            col("order_status")).where("order_status == 'delivered'")
            orders_original.createOrReplaceTempView("orders_original")
            val orders_delayed_deliveries_10Days_option1 = orders_original
                                        .withColumn("delivery_delay_in_days",
                                        datediff(col("order_delivered_customer_date"),
                                        col("order_purchase_timestamp")))
            orders_delayed_deliveries_10Days_option1.createOrReplaceTempView("orders_delayed_deliveries_10Days_option1")
            spark.sql("SELECT * FROM orders_delayed_deliveries_10Days_option1 where delivery_delay_in_days > 10")
        
       }
        // Run current function
        val DfExported = orders_delayed_deliveries_10Days(read_csvfile)
        // Sanity check to ensure the result is definitely correct
        if(testing == "false"){
            println("************Sanity check...************")
            println(DfExported.filter(col("delivery_delay_in_days") <=10).count == 0)
            println("************Sanity check done...************")
            // Export
        
            println("************Exporting to csv file...************")
            DfExported.coalesce(1).write.mode("overwrite")
                                    .format("com.databricks.spark.csv")
                                    .option("header","true").csv(s"$exportfile/option1_export")  
            println("************Exporting to csv file done...************")
        }
        //DfExported.write.option("compression", "gzip").mode("Overwrite").parquet(export_file)
        DfExported
    }

    def option2(spark: SparkSession, csvfile: String, exportfile: String, testing: String): DataFrame = {
       import spark.implicits._
       import org.apache.spark.sql._

       def read_csvfile = spark.read.option("header", true).csv(csvfile)

       def orders_delayed_deliveries_10Days(df: DataFrame) = {
            val orders_original = df.select(col("order_id"),
                                            col("customer_id"),
                                            col("order_purchase_timestamp"),
                                            col("order_delivered_customer_date"),
                                            col("order_status"),
                                            lit("America/Sao_Paulo").as("timezone")).where("order_status == 'delivered'")
            orders_original.createOrReplaceTempView("orders_original")
            val orders_UTC_timezone = orders_original
                                        .withColumn("order_purchase_timestamp_UTC", 
                                        to_utc_timestamp(col("order_purchase_timestamp"), col("timezone")))
                                        .withColumn("order_delivered_customer_date_UTC",
                                         to_utc_timestamp(col("order_delivered_customer_date"), col("timezone")))
                                        .drop("timezone")
                                        .drop("order_purchase_timestamp")
                                        .drop("order_delivered_customer_date")
            orders_UTC_timezone.createOrReplaceTempView("orders_UTC_timezone")
            val orders_UTC_DST_timezone = orders_UTC_timezone
                                .withColumn("daylight_saving_time", when(col("order_delivered_customer_date_UTC")
                                .between("2016-02-21 00:00:00","2016-10-16 00:00:00") ,"Winter_time")
                                .when(col("order_delivered_customer_date_UTC")
                                .between("2017-02-19 00:00:00","2017-10-15 00:00:00") ,"Winter_time")
                                .when(col("order_delivered_customer_date_UTC")
                                .between("2018-02-18 00:00:00","2018-11-04 00:00:00") ,"Winter_time")
                                .otherwise("Summer_time"))
            orders_UTC_DST_timezone.createOrReplaceTempView("orders_UTC_DST_timezone")
            spark.sql("SELECT * FROM orders_UTC_DST_timezone")
            val orders_delayed_deliveries_10Days_option2 = orders_UTC_DST_timezone
                                .withColumn("delivery_delay_in_days",
                                datediff(col("order_delivered_customer_date_UTC"),
                                col("order_purchase_timestamp_UTC")))
                                .withColumn("order_delivered_customer_date",date_format(col("order_delivered_customer_date_UTC"), "yyyy-MM-dd HH:mm:ss"))
                                .withColumn("order_purchase_timestamp",date_format(col("order_purchase_timestamp_UTC"), "yyyy-MM-dd HH:mm:ss"))
                                .drop("order_delivered_customer_date_UTC")
                                .drop("order_purchase_timestamp_UTC")
            orders_delayed_deliveries_10Days_option2.createOrReplaceTempView("orders_delayed_deliveries_10Days_option2")
            spark.sql("""SELECT order_id,
                        customer_id,
                        order_purchase_timestamp,
                        order_delivered_customer_date,
                        order_status,
                        daylight_saving_time,
                        delivery_delay_in_days 
                        FROM orders_delayed_deliveries_10Days_option2 where delivery_delay_in_days > 10""")
           
       }

        // Run current function
        val DfExported = orders_delayed_deliveries_10Days(read_csvfile)
        // Sanity check to ensure the result is definitely correct
        if(testing == "false"){
            println("************Sanity check...************")
            println(DfExported.filter(col("delivery_delay_in_days") <=10).count == 0)
            println("************Sanity check done...************")
            // Export
        
            println("************Exporting to csv file...************")
            DfExported.coalesce(1).write.mode("overwrite")
                                    .format("com.databricks.spark.csv")
                                    .option("header","true").csv(s"$exportfile/option2_export")  
            println("************Exporting to csv file done...************")
        }
        DfExported
    }

    def report_option1(spark: SparkSession, csvfile: String, testing: String) : DataFrame = {
       import spark.implicits._
       import org.apache.spark.sql._

       def read_csvfile = spark.read.option("header", true).csv(csvfile)

        def orders_delayed_deliveries_10Days(df: DataFrame) = {
                val orders_original = df.select(col("order_id"),
                                                col("customer_id"),
                                                col("order_purchase_timestamp"),
                                                col("order_delivered_customer_date"),
                                                col("order_status")).where("order_status == 'delivered'")
                orders_original.createOrReplaceTempView("orders_original")
                val orders_delayed_deliveries_10Days_option1 = orders_original
                                            .withColumn("delivery_delay_in_days",
                                            datediff(col("order_delivered_customer_date"),
                                            col("order_purchase_timestamp")))
                orders_delayed_deliveries_10Days_option1.createOrReplaceTempView("orders_delayed_deliveries_10Days_option1")
                println("************Report...************")
                orders_delayed_deliveries_10Days_option1.show
                spark.sql("SELECT * FROM orders_delayed_deliveries_10Days_option1 where delivery_delay_in_days > 10")
                //
        }

            // Run current function
            val DfExported = orders_delayed_deliveries_10Days(read_csvfile)
            if(testing == "false"){
                // Count rows
                println("************Count rows...************")
                println(DfExported.count())   // rows = 46604 
                println("************Count rows done...************")
                // Sanity check to ensure the result is definitely correct
                println("************Sanity check...************")
                println(DfExported.filter(col("delivery_delay_in_days") <=10).count == 0)
                println("************Sanity check done...************")
            }
            DfExported
        
    }

    def report_option2(spark: SparkSession, csvfile: String, testing: String) : DataFrame = {
       import spark.implicits._
       import org.apache.spark.sql._

       def read_csvfile = spark.read.option("header", true).csv(csvfile)

       def orders_delayed_deliveries_10Days(df: DataFrame) = {
            val orders_original = df.select(col("order_id"),
                                            col("customer_id"),
                                            col("order_purchase_timestamp"),
                                            col("order_delivered_customer_date"),
                                            col("order_status"),
                                            lit("America/Sao_Paulo").as("timezone")).where("order_status == 'delivered'")
            orders_original.createOrReplaceTempView("orders_original")
            val orders_UTC_timezone = orders_original
                                        .withColumn("order_purchase_timestamp_UTC", 
                                        to_utc_timestamp(col("order_purchase_timestamp"), col("timezone")))
                                        .withColumn("order_delivered_customer_date_UTC",
                                         to_utc_timestamp(col("order_delivered_customer_date"), col("timezone")))
                                        .drop("timezone")
                                        .drop("order_purchase_timestamp")
                                        .drop("order_delivered_customer_date")
            orders_UTC_timezone.createOrReplaceTempView("orders_UTC_timezone")
            val orders_UTC_DST_timezone = orders_UTC_timezone
                                .withColumn("daylight_saving_time", when(col("order_delivered_customer_date_UTC")
                                .between("2016-02-21 00:00:00","2016-10-16 00:00:00") ,"Winter_time")
                                .when(col("order_delivered_customer_date_UTC")
                                .between("2017-02-19 00:00:00","2017-10-15 00:00:00") ,"Winter_time")
                                .when(col("order_delivered_customer_date_UTC")
                                .between("2018-02-18 00:00:00","2018-11-04 00:00:00") ,"Winter_time")
                                .otherwise("Summer_time"))
            orders_UTC_DST_timezone.createOrReplaceTempView("orders_UTC_DST_timezone")
            spark.sql("SELECT * FROM orders_UTC_DST_timezone")
            val orders_delayed_deliveries_10Days_option2 = orders_UTC_DST_timezone
                                .withColumn("delivery_delay_in_days",
                                datediff(col("order_delivered_customer_date_UTC"),
                                col("order_purchase_timestamp_UTC")))
                                .withColumn("order_delivered_customer_date",date_format(col("order_delivered_customer_date_UTC"), "yyyy-MM-dd HH:mm:ss"))
                                .withColumn("order_purchase_timestamp",date_format(col("order_purchase_timestamp_UTC"), "yyyy-MM-dd HH:mm:ss"))
                                .drop("order_delivered_customer_date_UTC")
                                .drop("order_purchase_timestamp_UTC")
            orders_delayed_deliveries_10Days_option2.createOrReplaceTempView("orders_delayed_deliveries_10Days_option2")
            println("************Report...************")
            orders_delayed_deliveries_10Days_option2.show
            spark.sql("""SELECT order_id,
                        customer_id,
                        order_purchase_timestamp,
                        order_delivered_customer_date,
                        order_status,
                        daylight_saving_time,
                        delivery_delay_in_days 
                        FROM orders_delayed_deliveries_10Days_option2 where delivery_delay_in_days > 10""")
           
       }
            

        // Run current function
        val DfExported = orders_delayed_deliveries_10Days(read_csvfile)
        if(testing == "false"){
            // Count rows
            println("************Count rows...************")
            println(DfExported.count())  // rows = 46722
            println("************Count rows done...************")
            // Sanity check to ensure the result is definitely correct
            println("************Sanity check...************")
            println(DfExported.filter(col("delivery_delay_in_days") <=10).count == 0)
            println("************Sanity check done...************")
        }
        DfExported
        
    }
}