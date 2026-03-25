from ast import expr
import configparser
import time
import findspark
import utils
findspark.init()
from utils import create_session, sendmail
from pyspark.sql.functions import col, lit, struct, when,col, struct, when, rand ,concat_ws
from datetime import datetime 
from pyspark.sql.types import ArrayType, StringType
import logging
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, MapType ,FloatType
from pyspark.sql.types import IntegerType, DoubleType
import os
from logger import log_audit_event, setup_audit_logging, setup_file_logger, setup_db_logging, file_logger, db_logger

# Initialize loggers
file_logger = setup_file_logger()
db_logger = setup_db_logging()
audit_logger = setup_audit_logging()
 
file_logger = logging.getLogger('fileLogger')
db_logger = logging.getLogger('dblogger')
audit_logger = logging.getLogger('auditLogger')

#Read config file
config = configparser.ConfigParser()
config.read("config.ini")

#boto3 client
s3 = utils.s3

#source and destination locations for python file
source_dir = config.get('file_locations','s_location')
destination_dir = config.get('file_locations','d_location')

# S3  credentials
aws_access_key_id = config.get("aws", "aws_access_key_id")
aws_secret_access_key = config.get("aws", "aws_secret_access_key")
bucket_name = config.get("aws", "bucket_name")

# Spark Session
spark = create_session()

# mongoDB  credentials
mongo_uri = config.get("mongo", "mongo_uri")
mongo_collection = config.get("mongo", "mongo_collection_call")
mongo_database = config.get("mongo", "mongo_database")

class CallExtract:
    def mongo_call(self):
        try:
            file_logger.info("Starting the mongo_call_read process....")
            start_time = time.time()

            # Define the schema with nullable fields to handle missing data
            schema = StructType([
                StructField("request_id", IntegerType(), True),
                StructField("agent_id_id", IntegerType(), True),
                StructField("customer_id_id", IntegerType(), True),
                StructField("organisation_id_id", IntegerType(), True),
                StructField("interaction_id", StringType(), True),
                StructField("duration", StringType(), True),
                StructField("processes", StringType(), True),
                StructField("tone_data", StructType([
                    StructField("Sentiment", StructType([
                        StructField("agent_sentiment", StructType([
                            StructField("NEGATIVE", IntegerType(), True),
                            StructField("NEUTRAL", DoubleType(), True),
                            StructField("POSITIVE", DoubleType(), True)
                        ]), True),
                        StructField("customer_sentiment", StructType([
                            StructField("NEGATIVE", DoubleType(), True),
                            StructField("NEUTRAL", DoubleType(), True),
                            StructField("POSITIVE", DoubleType(), True)
                        ]), True)
                    ]), True),
                    StructField("transcript", StructType([
                        StructField("full", MapType(StringType(), StringType()), True),
                        StructField("AGENT", MapType(StringType(), ArrayType(StringType())), True),
                        StructField("CUSTOMER", MapType(StringType(), ArrayType(StringType())), True)
                    ]), True),
                    StructField("keywords_tracking", StructType([
                        StructField("agent_keywords_tracking", MapType(StringType(), MapType(StringType(), IntegerType())), True),
                        StructField("customer_keywords_tracking", MapType(StringType(), MapType(StringType(), IntegerType())), True)
                    ]), True),
                    StructField("dead_air_res", StringType(), True),
                    StructField("level_1", StringType(), True),
                    StructField("agent_final_tone", StringType(), True),
                    StructField("cust_final_tone", StringType(), True),
                ]), True),
                StructField("compliance_data", StructType([
                    StructField("average_maximum_score", DoubleType(), True),
                    StructField("call_tagging", StructType([
                        StructField("tag", StringType(), True),
                        StructField("reason_of", StringType(), True)
                    ]), True)
                ]), True),
                StructField("original_date", StringType(), True),
                StructField("language_detection", StringType(), True)
            ])

            # Create an empty DataFrame with the schema
            final_call_df = spark.createDataFrame([], schema)

            file_logger.info("empty final_call_df created.")
            
            # Define columns to read
            columns_to_read = ["request_id", "agent_id_id", "customer_id_id", "organisation_id_id","interaction_id",
                               "duration","processes", "tone_data.Sentiment",
                                "tone_data.transcript", "tone_data.keywords_tracking", "compliance_data.call_tagging.reason_of",
                               "tone_data.agent_final_tone", "tone_data.cust_final_tone",
                               "tone_data.dead_air_res", "tone_data.level_1","compliance_data.average_maximum_score",
                               "compliance_data.call_tagging.tag","original_date","language_detection"]

            pipeline = [
                        {"$match": {"process_status": "Process Completed"}},  # Only filter by process status
                        {"$project": {column: 1 for column in columns_to_read}}  # Include the specified columns
                    ]

            df = spark.read.format("mongo").option("uri", mongo_uri) \
                            .option('spark.mongodb.input.database', mongo_database) \
                            .option("collection", mongo_collection) \
                            .option("pipeline", str(pipeline)) \
                            .option("multiline", "true") \
                            .load() 
            
            file_logger.info(f"Fetching call data from MongoDB as df")

            # Check if the DataFrame is empty
            if df.isEmpty():
                file_logger.info(f"No data found in MongoDB  for call")
                db_logger.info(f"No data found in MongoDB for call")
                log_audit_event(stage="Call_MongoDB", action="Read", status="Failed", description="No data found in MongoDB for call", duration=time.time() - start_time)
                sendmail(f"No data found in MongoDB for the for call")
                print(f"No data found for call. Exiting process.")
                return

            df.cache()   #optimization
            
            # Count records and log the record count
            record_count = df.count()
            print(f"Data read successfully count is {record_count}")
            file_logger.info(f"Number of call records fetched from MongoDB: {record_count}")
            db_logger.info(f"Number of call records fetched from MongoDB: {record_count}")
            log_audit_event(stage="Call_MongoDB", action="Read", status="Success", description=f"Fetched {record_count} records from MongoDB for call", duration=time.time() - start_time)

            # flatting df for nested json structure
             
            flattened_df = df.select(col("request_id").alias("call_id"),
                                     col("interaction_id").alias("interaction_file_id").cast('string'),
                                     col("agent_id_id").alias("agent_id"),
                                     col("customer_id_id").alias("client_id"),
                                     col("organisation_id_id").alias("organisation_id"),
                                     col("processes").cast("string").alias("processes"),
                                     col("duration").alias("interaction_duration"), 
                                     when(col("tone_data.dead_air_res").isNull(), lit("")).otherwise(col("tone_data.dead_air_res").cast("string")).alias("dead_air"),   
                                     col("tone_data.Sentiment").alias("Sentiment"),
                                     col("tone_data.agent_final_tone").alias("agent_final_tone"),
                                     col("tone_data.cust_final_tone").alias("cust_final_tone"),
                                     col("tone_data.transcript.AGENT").alias("agent"),
                                     col("tone_data.transcript.CUSTOMER").alias("customer"),
                                     col("tone_data.transcript.full").alias("full"),
                                     col("compliance_data.average_maximum_score").alias("average_compliance_score"),
                                     col("compliance_data.call_tagging.tag").alias("ai_feedback"),
                                     col("compliance_data.call_tagging.reason_of").alias("ai_feedback_reason"),
                                     col("tone_data.level_1").alias("customer_intent"),
                                     col("original_date").alias("interaction_date"),
                                     col("language_detection").alias("language_detection"),
                                     col("tone_data.transcript.full.transcript_for_summary").alias("transcript_summary"),
                                     col("tone_data.keywords_tracking")).alias("keywords_tracking")

            file_logger.info("Data flattened successfully.")
            # print("Data flattened successfully.")
            # flattened_df.printSchema()

            #concatenating the values of the columns processes and interaction_duration into single strings.
            modified_df = flattened_df.withColumn("processes", concat_ws(",", col("processes")))\
                                     .withColumn("interaction_duration", concat_ws(",", col("interaction_duration")))
            
            # Union the empty DataFrame with the data read from MongoDB
            final_call_df = final_call_df.unionByName(modified_df, allowMissingColumns=True)
            # print(final_call_df.printSchema())
            # print("Union the empty DataFrame")

            df_with_defaults = final_call_df \
                                    .withColumn("call_status", lit("received").cast("string")) \
                                    .withColumn("wait_time", lit("00:00:00.00").cast("string")) \
                                    .withColumn("hold_time", lit("00:00:00.00").cast("string")) \
                                    .withColumn("self_service", lit("no").cast("string")) \
                                    .withColumn("product_id", lit(1).cast("int")) \
                                    .withColumn("customer_satisfaction_rates", (rand() * 4 + 1).cast("string")) \
                                    .withColumn("customer_effort_score", (rand() * 4 + 1).cast("string")) \
                                    .withColumn("NPS_Score", (rand() * 4 + 1).cast("string")) \
                                    .withColumn("customer_rating", (rand() * 4 + 1).cast("string")) \
                                    .withColumn("escalation", when(rand() > 0.5, lit("yes")).otherwise(lit("no"))) \
                                    .withColumn(
                                        "escalation_reason",
                                        when(
                                            col("escalation") == "yes",
                                            when((rand() * 4).cast("int") == 0, lit("billing dispute"))
                                            .when((rand() * 4).cast("int") == 1, lit("agent incompetency"))
                                            .when((rand() * 4).cast("int") == 2, lit("service delay"))
                                            .otherwise(lit("policy constrain"))
                                        ).otherwise(lit(""))
                                    ) \
                                    .withColumn("manual_feedback", lit("").cast("string")) \
                                    .withColumn("manual_rating", lit("").cast("string")) \
                                    .withColumn("channel_id", lit("1").cast("string")) \
                                    .withColumn("department", lit("customer_support").cast("string")) \
                                    .withColumn("transfered_call", lit("").cast("string"))
            
            # Add a new column with random values between 1 and 1001 for customer_id
            df_with_customer_id = df_with_defaults.withColumn("customer_id", (rand() * 1000 + 1).cast("int"))

            # print(f"added new columns successfully")
            # order column in rrequired order
            df_with_correct_order = df_with_customer_id.select(
                col("call_id"),
                col("interaction_file_id"),
                col("agent_id"),
                col("client_id"),
                col("customer_id"),
                col("organisation_id"),
                col("self_service"),
                col("product_id"),
                col('channel_id'),
                col("department"),
                col("processes"),
                col("agent_final_tone"),
                col("cust_final_tone"),
                col("interaction_duration"),
                col("dead_air"),
                col("average_compliance_score"),
                col("ai_feedback"),
                col("ai_feedback_reason"),
                col("customer_intent"),
                col("escalation"),
                col("escalation_reason"),
                col("manual_feedback"),
                col("manual_rating"),
                col("customer_rating"),
                col("interaction_date"),
                col("agent"),
                col("customer"),
                col("full"),
                col("Sentiment"),
                col("transcript_summary"),
                col("keywords_tracking"),
                col("language_detection"),
                col("call_status"),
                col("wait_time"),
                col("hold_time"),
                col("transfered_call"),
                col("customer_satisfaction_rates"),
                col("customer_effort_score"),
                col("NPS_Score"))
            
            # df_with_correct_order.show(n=10,truncate = False)
            # df_with_correct_order.printSchema()
            
            # nesting columns as per requirement for transcript and call_data
            nested_transcript = ["full","agent","customer"]
            new_spark_df = df_with_correct_order.withColumn("transcript", struct(*nested_transcript))
            df_with_transcript = new_spark_df.drop(*nested_transcript)

            nested_call_data = ["call_status", "wait_time", "hold_time","transfered_call", 
                                "language_detection", "transcript", "transcript_summary", 
                                "keywords_tracking", "Sentiment", "agent_final_tone" , "cust_final_tone",
                                 "customer_satisfaction_rates", "customer_effort_score", "NPS_Score"]
            new_spark_df1 = df_with_transcript.withColumn("call_data", struct(*nested_call_data))
            df_with_call_data = new_spark_df1.drop(*nested_call_data)
            
            # print(df_with_call_data.printSchema())
            
            # saving df as json at local
            folder_name = 'call_data'
            file_name = 'call_data_' + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

            # Direct flatten data write to db:
            # writing data in to source dir
            df_with_call_data.coalesce(1).write.option("nullValue","null").option("ignoreNullFields","false").mode('overwrite').json(os.path.join(source_dir,folder_name,file_name))
            destination_file_path = f"{destination_dir}/call_data"
            # rename file and copy file to final destination
            json_file_path = utils.copy_and_rename_file(source_dir,folder_name,destination_file_path)
            

            db_logger.info(f"call Data successfully saved to {json_file_path}")
            file_logger.info(f"call Data successfully saved to {json_file_path}")
            log_audit_event(stage="Call_Local", action="Write", status="Success", description=f"Saved {record_count} Call records to local path {json_file_path}", duration=time.time() - start_time)
            print(f"call data saved successfully")

        except Exception as e:
            file_logger.error(f"Error occurred during the mongo_call process: {str(e)}")
            sendmail("Failed to read call data", "call file generation", f"Failed to read call data: {str(e)}")
            log_audit_event(stage="Call_MongoDB", action="Read", status="Failed", description=f"Error: {str(e)}", duration=time.time() - start_time)

