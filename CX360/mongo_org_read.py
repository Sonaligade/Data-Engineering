import configparser
import json
import time
import findspark
from pyspark.sql.functions import col, lit, array, struct
from datetime import datetime
import os
from utils import create_session, sendmail
from final_models import Organisation
import logging
from logger import log_audit_event, setup_file_logger, setup_db_logging, file_logger, db_logger
import utils


# Initialize loggers
file_logger = setup_file_logger()
db_logger = setup_db_logging() 

file_logger = logging.getLogger('fileLogger')
db_logger = logging.getLogger('dblogger')

config = configparser.ConfigParser()
config.read("config.ini")

#boto3 client and AWS credencials
s3 = utils.s3
bucket_name = config.get("aws", "bucket_name")


# Spark session
spark = create_session()

# MongoDB credentials
mongo_uri = config.get("mongo", "mongo_uri")
mongo_collection = config.get("mongo", "mongo_collection_org")
mongo_database = config.get("mongo", "mongo_database")

#source and destination locations for python file
source_dir = config.get('file_locations','s_location')
destination_dir = config.get('file_locations','d_location')

class Dim_Org1:
    def mongo_org(self):
        file_logger.info("Starting to read organization data from MongoDB")
        start_time = time.time()
        try:
            df = spark.read.format("mongo").option("uri", mongo_uri) \
                            .option('spark.mongodb.input.database', mongo_database) \
                            .option("collection", mongo_collection) \
                            .option('mode', 'dropmalformed') \
                            .option('inferSchema', 'true') \
                            .load()
            
            if df.isEmpty():
                file_logger.info(f"No data found in MongoDB for organization")
                db_logger.info(f"No data found in MongoDB for organization")
                log_audit_event(stage="Organization_MongoDB", action="Read", status="Failed", description="No data found in MongoDB for organization", duration=time.time() - start_time)
                sendmail(f"No data found in MongoDB for organization")
                print(f"No data found for organization Exiting process.")
                return
            
            # Count records and log the record count
            record_count = df.count()
            file_logger.info(f"Number of records fetched from MongoDB: {record_count}")
            db_logger.info(f"Number of records fetched from MongoDB: {record_count}")
            log_audit_event(stage="Organization_MongoDB", action="Read", status="Success", description=f"Fetched {record_count} records from MongoDB for organisation", duration=time.time() - start_time)

            org_df = df.select(
                col("organisation_id"),
                col("organisation_name"),
                col("organisation_email"),
                col("organisation_mobile").cast('string'),
                col("organisation_address"),
                col("organisation_city"),
                col("organisation_country"),
                col("organisation_pincode").cast('string'),
                col("organisation_state")).na.fill({"organisation_pincode": 0})

           

            org_df = org_df.select(col("organisation_id"),
                                col("organisation_name"), 
                                col("organisation_address"), 
                                col("organisation_city"),
                                col('organisation_state'),
                                col("organisation_country"),
                                col("organisation_pincode"),
                                col("organisation_email"),
                                col("organisation_mobile"))

            # file and folder name of data
            folder_name = 'org_data'
            file_name = 'org_data_' + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            file_logger.info(f"writing data frame at {destination_dir}")
            # writing data in to source dir
            org_df.coalesce(1).write.option("nullValue",None).option("ignoreNullFields","false").mode('overwrite').json(os.path.join(source_dir,folder_name,file_name))
            destination_file_path = f"{destination_dir}/org_data"
            json_file_path = utils.copy_and_rename_file(source_dir,folder_name,destination_file_path)

            db_logger.info(f"Customer Data successfully saved to local at {json_file_path}")
            file_logger.info(f"Customer Data successfully saved to local at {json_file_path}")

        except Exception as e:
            file_logger.error(f"unknown error in fetching_user_data_from_mongo " +str(e), exc_info=True)
        else:
            file_logger.info(f"file created successfully {json_file_path}")
