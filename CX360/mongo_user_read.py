from pyspark.sql.functions import col,lit,concat_ws
from pyspark.sql.types import IntegerType, ArrayType, StringType
import configparser
from datetime import datetime
import os
import shutil
import glob
import utils
from logger import log_audit_event, setup_file_logger, setup_db_logging, file_logger, db_logger
import logging


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

# MongoDB credentials
mongo_uri = config.get("mongo", "mongo_uri")
mongo_collection_user = config.get("mongo", "mongo_collection_user")
mongo_collection_role = config.get("mongo","mongo_collection_role")
mongo_database = config.get("mongo", "mongo_database")

#source and destination locations for python file
source_dir = config.get('file_locations','s_location')
destination_dir = config.get('file_locations','d_location')

spark = utils.create_session()


# Read user data from MongoDB
class user_data:
    def fetching_user_data_from_mongo(self):
        try:
            file_logger.info("Function fetching_user_data_from_mongo is started")

            # Read user data from MongoDB
            
            user_df = spark.read.format("mongo").option("uri", mongo_uri) \
                            .option('spark.mongodb.input.database', mongo_database) \
                            .option("collection", mongo_collection_user) \
                            .option('mode', 'dropmalformed') \
                            .option('inferSchema', 'true') \
                            .load()

            file_logger.info("Reading role data from MongoDB")
            # Read role data from MongoDB
            pipeline = "[{'$project': {'role_id': 1, 'role_name': 1}}]"
            role_df = spark.read.format("mongo") \
                           .option("uri", mongo_uri) \
                           .option("database", mongo_database) \
                           .option("collection", mongo_collection_role) \
                           .option("mode", "dropmalformed") \
                           .option("inferSchema", "true") \
                           .option('pipeline', pipeline) \
                           .load()

           
            # Joining the DataFrames for `role_name` column
            user_df = user_df.join(role_df, user_df.role_id_id == role_df.role_id, 'left')

            select_col = {'user_table': ['user_id', 'first_name', 'last_name', 'email', 'date_joined',
                                         'user_email', 'role_id_id', 'role_name', 'qa_id', 'manager_id',
                                         'customer_id_id', 'organisation_id_id', 'processes', 'empcode',
                                         'user_mobile', 'tenure', 'bucket', 'status', 'is_active']}

            file_logger.info("Selecting and renaming columns")
            # Selecting and renaming columns
            user_df = user_df.select(select_col['user_table']).withColumnRenamed('role_id_id', 'role_id') \
                .withColumnRenamed('customer_id_id', 'customer_id') \
                .withColumnRenamed('organisation_id_id', 'organisation_id')

            file_logger.info("Casting and transforming columns")
            # Casting columns
            user_df = user_df.withColumn('qa_id', col('qa_id').cast(IntegerType())) \
                .withColumn('manager_id', col('manager_id').cast(IntegerType())) \
                .withColumn('date_joined', col('date_joined').cast(StringType())) \
                .withColumn('processes', concat_ws(',', col('processes'))) \
                .withColumn('is_active', col('is_active').cast(StringType()))

            # file and folder name of data
            folder_name = 'user_data'
            file_name = 'user_data_' + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            file_logger.info(f"writing data frame at {destination_dir}")
            # writing data in to source dir
            user_df.coalesce(1).write.option("nullValue",None).option("ignoreNullFields","false").mode('overwrite').json(os.path.join(source_dir,folder_name,file_name))
            destination_file_path = f"{destination_dir}/user_data"
            json_file_path = utils.copy_and_rename_file(source_dir,folder_name,destination_file_path)

            db_logger.info(f"Customer Data successfully saved to local at {destination_dir}")
            file_logger.info(f"Customer Data successfully saved to local at {destination_dir}")

        except Exception as e:
            file_logger.error(f"unknown error in fetching_user_data_from_mongo " +str(e), exc_info=True)
        else:
            file_logger.info(f"file created successfully {json_file_path}")

