import findspark
findspark.init()
from fastapi import HTTPException
import configparser
from pyspark.sql import SparkSession
import json
import boto3
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import sys
import os
import mysql.connector
import logging
from logger import setup_file_logger, setup_db_logging, file_logger, db_logger
import hashlib
import shutil
import glob
import subprocess
from pathlib import Path
from pyspark.sql.utils import AnalysisException

# Initialize loggers
file_logger = setup_file_logger()  # Initialize file logger
db_logger = setup_db_logging()   # Initialize DB logger

file_logger = logging.getLogger('fileLogger')
db_logger = logging.getLogger('dblogger')


#reading config file
config = configparser.ConfigParser()
config.read("config.ini")



# ------------------- S3  credentials

try: 
    aws_access_key_id = config.get("aws", "aws_access_key_id")
    aws_secret_access_key = config.get("aws", "aws_secret_access_key")
    bucket_name = config.get("aws", "bucket_name")
except Exception as e:
    file_logger.error("Error in fetching cloud credentials: " + str(e), exc_info=True)


# -------------------- mysql database credentials
try: 
    url = config.get("sql", "url")
    host = config.get("sql", "host")
    user = config.get("sql", "user")
    password = config.get("sql", "password")
    database = config.get("sql", "database")
except Exception as e:
    file_logger.error("Error in fetching database credentials: " + str(e), exc_info=True)

mysql_properties = {"user": config.get("sql", "user"), "password": config.get("sql", "password"), "driver": "com.mysql.jdbc.Driver"}


# -------------------- creating BOTO3 client
try: 
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
except Exception as e:
    file_logger.error("Error in connecting with boto3 client: " + str(e), exc_info=True)


# -----------------------------creating sendmail function----------------------------------------------------------------
import smtplib
from email.mime.text import MIMEText
from datetime import datetime

# Fetch email configuration from the config file
sender_email = config.get("email", "sender_email")
recipient_emails = [email.strip() for email in config.get("email", "recipient_list").split(',')]  # Split and strip spaces
smtp_server = config.get("email", "smtp_server")
smtp_port = config.getint("email", "smtp_port")  # Fetch as an integer The smtp_port is fetched as an integer using config.getint() because the port number is an integer value, not a string.
app_password = config.get("email", "app_password")
project_name=config.get("email", "project_name")
pipeline_name=config.get("email", "pipeline_name")
body=config.get("email","body")

def sendmail(subject, stage_name, message, execution_time=None):
    # If execution_time is not provided, use the current time
    if execution_time is None:
        execution_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Replace placeholders in the body with actual values
    email_body = body.format(
        project_name=project_name,
        pipeline_name=pipeline_name,
        execution_time=execution_time,
        stage_name=stage_name,
        message=message
    )
    
    # Create message in HTML format
    msg = MIMEText(email_body, 'html')
    msg['From'] = sender_email
    msg['To'] = ', '.join(recipient_emails)
    msg['Subject'] = subject

    # Send email
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Start TLS encryption
            server.login(sender_email, app_password)
            server.sendmail(sender_email, recipient_emails, msg.as_string())
            # print("Email regarding pipeline execution sent successfully!")
    except Exception as e:
        print(f"Error while sending message to email: {e}")

# ------------------------------creating spark session-------------------------------------------------------
def create_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName('my_app') \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memoryOverhead", "2g") \
        .config("spark.network.timeout", "600s") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.files.maxPartitionBytes", 262144)\
        .config("spark.sql.shuffle.partitions", 200) \
        .config("spark.dynamicAllocation.enabled", True) \
        .config("spark.dynamicAllocation.minExecutors", 2) \
        .config("spark.dynamicAllocation.maxExecutors", 10) \
        .getOrCreate()

    spark.conf.set("spark.sql.caseSensitive", "true")
    
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", aws_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", aws_secret_access_key)
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    return spark
# -------------------class to read data from mongo--------------------------------------------------------------------------------------

class ScriptExecutor:
    def __init__(self, script_names):
        """
        Initializes the ScriptExecutor with a list of script names.
        :param script_names: List of script names to be executed.
        """
        self.script_names = script_names

    def execute_script(self, script_name):
        """
        Executes a single script by its name.
        :param script_name: Name of the script to execute.
        """
        try:
            print(f"Executing {script_name}...")
            subprocess.run(["python", script_name], check=True)
            print(f"{script_name} executed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error while executing {script_name}: {e}")
        except FileNotFoundError:
            print(f"Script {script_name} not found in the current directory.")

    def execute_all(self):
        """
        Executes all scripts in the script_names list.
        """
        for script in self.script_names:
            self.execute_script(script)

# -------------------function to load data to database----------------------------------------------------------------------------------
def to_database(df, table_name,batch_size):
    try:
        df.write.option("batchsize", batch_size).jdbc(url=url,
                      table=table_name,
                      mode="append",
                      properties=mysql_properties)
        logging.info(f"file successfully loaded in {table_name}")
        
    except Exception as e:
        file_logger.error("Error in connecting to database, which resulted in file not loading to database table " + table_name + ": " + str(e), exc_info=True)
        raise HTTPException(status_code=400, detail=f"File not loaded to database table {table_name}: {str(e)}")

# --------------------function to archive data to S3----------------------------------------------------------------------------------------
def to_archive(org_name, folder):
    try:
        prefix = org_name + '/' + folder + '/'
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=org_name + '/' + folder + '/')
        file_keys = [obj['Key'] for obj in response.get('Contents', [])]
        for file_key in file_keys:
            file_n = file_key.split("/")[-1]

            s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': prefix + file_n}, Bucket=bucket_name,
                           Key=org_name + "/" + folder + "_archive/" + file_n)
            s3.delete_object(Bucket=bucket_name, Key=prefix + file_n)
    except Exception as e:
        file_logger.error("Error in archiving data: " + str(e), exc_info=True)
        raise HTTPException(status_code=400, detail=f"File archive error : {str(e)}")

# ----------------------------UDF to Flatten Dynamically-----------------------------------------------------------------------

def flatten_array_of_structs(array):
    if array is None:
        return None
    return "; ".join(
        [
            ", ".join([f"{k}" for k, v in struct.asDict().items() if v is not None])
            for struct in array
            if struct is not None
        ]
    )

# ----------------function to check file size before uploading file into s3 bucket---------------------------------------------------------
def check_size(data):
    try:
        size = len(data)  # Assuming `data` is a byte object
        if size == 0:
            return False
        else:
            return True
    except Exception as e:
        file_logger.error("Error in checking file size: " + str(e), exc_info=True)
 
# --------------------------------function to assign hash value before uploading file to s3 bucket--------------------------------------------------------
def calculate_file_hash(file_content):
    """Calculate a hash for the given file content."""
    return hashlib.md5(file_content).hexdigest()

# ---------------------------------function to check duplication of files with s3 bucket--------------------------------------------------
# Check if a file with the same hash exists in S3 by searching for its hash in the file name.
def check_file_exists_by_hash(org_name, data_type, file_hash):
    s3_prefix = f"{org_name}/{data_type}/"
    s3_file_name = f"{data_type}_data_{file_hash}.json"
    s3_object_key = f"{s3_prefix}{s3_file_name}"

    try:
        s3.head_object(Bucket=bucket_name, Key=s3_object_key)
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise e

# ---------------------------------------UDF for extracting maximum sentiment & its value--------------------------------------------------
def get_max_value(s):
    if s is None:
        return None
    else:
        try:
            d = json.loads(s)
            max_key = None
            max_value = float('-inf')

            for key, value in d.items():
                if value > max_value:
                    max_key = key
                    max_value = value

            if max_key is not None:
                return [max_key, max_value]
            else:
                return None
        except Exception as e:
            file_logger.error("Error in get_max_value function: " + str(e), exc_info=True)

# --------------------------------- Function to archive data to local file system -----------------------------------------------------

def data_to_archive(data_file_name):
    try:
        cwd = os.getcwd()
        src = os.path.join(cwd, "data_files", "valid_data", f"{data_file_name}_data")
        
        arv = os.path.join(cwd, "data_files", "archive")
        os.makedirs(arv, exist_ok=True)

        des = os.path.join(arv, f"{data_file_name}_data")
        os.makedirs(os.path.join(arv, f"{data_file_name}_data"), exist_ok=True)

        for filename in os.listdir(src):
            try:
                shutil.move(src, os.path.join(des, filename))
                print('Files successfully archived')
            except Exception as e:
                print(f"Error in data movement for arvhiving: {e}")
    
    except Exception as e:
        print(f"Error in data archive function: {e}")
        
# ---------------------copy and rename file from source to final load------------------------------------------------------------------
def copy_and_rename_file(source_dir,folder_name,destination_dir):
    # Ensure the destination directory exists
    if os.path.exists(destination_dir) == False:
        os.makedirs(destination_dir)

    # Find the latest directory based on creation or modification time
    latest_dir = max(
        [os.path.join(source_dir, folder_name, d) for d in os.listdir(os.path.join(source_dir, folder_name))],
        key=os.path.getmtime)

    # Search the part file in source
    json_file_path = glob.glob(os.path.join(latest_dir, "part-*.json"))[0]

    # Set the new file name for the copy
    new_file_name = folder_name+ '_' + datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".json"

    # destinition file path
    destination_file_path = os.path.join(destination_dir, new_file_name)

    # copy and rename file
    shutil.copy(json_file_path, destination_file_path)

    return destination_file_path