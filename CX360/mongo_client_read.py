import configparser
import time
import findspark
import pyspark
import json
import os
import logging
from pyspark.sql.functions import col, lit, date_format
from datetime import datetime
from pyspark.sql.types import IntegerType
from utils import create_session, sendmail
#from final_models import Call, Agent, Customer, Organization
from logger import log_audit_event, setup_file_logger, setup_db_logging, file_logger, db_logger,setup_audit_logging
import utils

# Initialize loggers
file_logger = setup_file_logger()  # Initialize file logger
db_logger = setup_db_logging()   # Initialize DB logger
audit_loger = setup_audit_logging()

file_logger = logging.getLogger('fileLogger')
db_logger = logging.getLogger('dblogger')
audit_loger = logging.getLogger('auditLogger')

config = configparser.ConfigParser()
config.read("config.ini")

#boto3 client and AWS credencials
s3 = utils.s3
bucket_name = config.get("aws", "bucket_name")

# Spark session
spark = create_session()

# MongoDB credentials
mongo_uri = config.get("mongo", "mongo_uri")
mongo_collection = config.get("mongo", "mongo_collection_cust")
mongo_database = config.get("mongo", "mongo_database")

#source and destination locations for python file
source_dir = config.get('file_locations','s_location')
destination_dir = config.get('file_locations','d_location')

class client_data:
    def mongo_customer(self):
        file_logger.info("Starting to read customer data from MongoDB")
        start_time = time.time()
        try:
            df = spark.read.format("mongo").option("uri", mongo_uri) \
                            .option('spark.mongodb.input.database', mongo_database) \
                            .option("collection", mongo_collection) \
                            .option("mode","permissive")\
                            .option('inferSchema', 'true') \
                            .load()
            
            if df.isEmpty():
                file_logger.info(f"No data found in MongoDB for customer")
                db_logger.info(f"No data found in MongoDB for customer")
                log_audit_event(stage="Customer_MongoDB", action="Read", status="Failed", description="No data found in MongoDB for customer", duration=time.time() - start_time)
                sendmail(f"No data found in MongoDB for customer")
                print(f"No data found for customer Exiting process.")
                return

            # Count records and log the record count
            record_count = df.count()
            file_logger.info(f"Number of records fetched from MongoDB: {record_count}")
            db_logger.info(f"Number of records fetched from MongoDB: {record_count}")
            log_audit_event(stage="Customer_MongoDB", action="Read", status="Success", description=f"Fetched {record_count} records from MongoDB for Call", duration=time.time() - start_time)

            cust_df = df.select(
               col("customer_id").alias("client_id"),
               col("organisation_id_id").alias("organisation_id"),
               col("customer_first_name").alias("client_first_name"),
               col("customer_last_name").alias("client_last_name"),
               col("language").alias("client_language"), 
               col("customer_email").alias("client_email"),
               col("customer_mobile").alias("client_mobile")
            )

            # file and folder name of data
            folder_name = "client_data"
            file_name = 'client_data_' + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            file_logger.info(f"writing data frame at {destination_dir}")
            # writing data in to source dir
            cust_df.coalesce(1).write.option("nullValue",None).option("ignoreNullFields","false").mode('overwrite').json(os.path.join(source_dir,folder_name,file_name))
            destination_file_path = f"{destination_dir}/client_data"
            json_file_path = utils.copy_and_rename_file(source_dir,folder_name,destination_file_path)

            db_logger.info(f"Customer Data successfully saved to local at {json_file_path}")
            file_logger.info(f"Customer Data successfully saved to local at {json_file_path}")
            log_audit_event(stage="Customer_Local", action="Write", status="Success", description=f"Saved {record_count} customer records to local path {json_file_path}", duration=time.time() - start_time)
            print("Customer data saved successfully")

        except Exception as e:
            file_logger.error(f"Failed to read customer data: {str(e)}")
            sendmail("Failed to read customer data", "customer file generation", f"Failed to read customer data: {str(e)}")
            log_audit_event(stage="Customer_MongoDB", action="Read", status="Failed", description=f"Error: {str(e)}", duration=time.time() - start_time)
