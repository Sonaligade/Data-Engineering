from fastapi import FastAPI, HTTPException, Depends
import utils
import uvicorn
import shutil
import os
import json
from final_models import User, Client, Call, CustomerCRM, Organisation, Product, Channel
import mysql.connector
from user_dim import Dim_User
from org_dim import Dim_Org
from client_dim import Dim_Client
from prod_dim import Dim_product
from crm_dim import Dim_crm
from channel_dim import Dim_channel
from omni_channel_call import Channels
import logging
from logger import setup_file_logger, setup_db_logging, setup_audit_logging, file_logger, db_logger, log_audit_event
from utils import sendmail
from scd import user_scd, org_scd, client_scd, product_scd, crm_scd, channel_scd
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from mongo_call_read import CallExtract
from mongo_client_read import client_data
from mongo_org_read import Dim_Org1
from mongo_user_read import user_data
import configparser
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import json

app = FastAPI()


config = configparser.ConfigParser() 
config.read("config.ini") 

# Initialize loggers
file_logger = setup_file_logger()  # Initialize file logger
db_logger = setup_db_logging()   # Initialize DB logger
audit_logger = setup_audit_logging()

file_logger = logging.getLogger('fileLogger')
db_logger = logging.getLogger('dblogger')
audit_logger = logging.getLogger('auditLogger')

app = FastAPI()

# Database connection
url = utils.url
host = utils.host
user = utils.user
password = utils.password
database = utils.database
security = HTTPBearer()

# Get the security token from config
security_token = config.get("security", "token")

# Helper function to validate token
async def validate_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != security_token:
        raise HTTPException(status_code=403, detail="Invalid or missing token")

db_logger.info("New ETL Iteration")
file_logger.info("-------------------------New ETL Iteration -----------------------\n")
sendmail("cx360 ETL pipeline has started", "Initialization", "Pipeline Execution Initiated")

# API Endpoint to process all tasks (fetch, validate, transform)
@app.post("/cx360_pipeline/")
async def process_all_tasks(credentials: HTTPAuthorizationCredentials = Depends(validate_token)):
    try:
        # Step 1: Fetch data from MongoDB
        file_logger.info("Starting MongoDB data fetching...")
        db_logger.info("Starting MongoDB data fetching...")

        # Fetch user data
        agent = user_data()
        agent.fetching_user_data_from_mongo()

        # Fetch client data
        customer = client_data()
        customer.mongo_customer()

        # Fetch org data
        org = Dim_Org1()
        org.mongo_org()

        # Fetch call data
        call = CallExtract()
        call.mongo_call()

        file_logger.info("Data fetched from MongoDB successfully.")
        db_logger.info("Data fetched from MongoDB successfully.")

    except Exception as e:
        file_logger.error(f'Error during reading: {e}')
        db_logger.error(f'Error during reading: {e}')
        sendmail("Error in the transformation", "Transformation", f'Error during reading: {e}')
        os._exit(1)
        raise HTTPException(status_code=400, detail=f"Invalid JSON structure: {str(e)}")

    # Step 2: Validate data

    try:
        file_logger.info("Starting Data validation...")
        db_logger.info("Starting Data validation...")

        curr_dir = os.getcwd()
        final_loc = os.path.join(curr_dir, 'data_files', 'final_load')
        valid_folder = os.path.join(curr_dir, 'data_files', 'valid_data')
        invalid_folder = os.path.join(curr_dir, 'data_files', 'invalid_data')

        valid_data, invalid_data, error, result = [], [], 'No Error', ""

        # Ensure the necessary directories exist
        os.makedirs(final_loc, exist_ok=True)
        os.makedirs(valid_folder, exist_ok=True)
        os.makedirs(invalid_folder, exist_ok=True)
        
        pydantic_classes = {"user_data": User, "client_data": Client, "call_data": Call, "crm_data": CustomerCRM,
                            "org_data": Organisation, "product_data": Product, "channel_data": Channel}
        
        for key, value in pydantic_classes.items():
            folder_name = key
            folder_path = os.path.join(final_loc, folder_name)

            # Check if the folder exists before trying to list files in it
            if os.path.exists(folder_path) and os.path.isdir(folder_path):
                # Loop through each file in the directory
                for filename in os.listdir(folder_path):
                    src_file = os.path.join(folder_path, filename)

                    if os.path.isfile(src_file):
                        try:
                            # Read and load JSON data
                            with open(src_file, 'r', encoding='utf-8') as file:
                                json_data = [json.loads(line) for line in file]

                            # Data validation using the corresponding Pydantic class
                            data_validation = value(**{'records': json_data})

                            # Determine target folder based on validation
                            if data_validation:
                                target_folder = valid_folder
                                status = 'VALID'
                                valid_data.append(filename)
                            else:
                                target_folder = invalid_folder
                                status = 'INVALID'
                                invalid_data.append(filename)

                            # Ensure the target subdirectory exists
                            subfolder = os.path.join(target_folder, folder_name)
                            if not os.path.exists(subfolder):
                                os.makedirs(subfolder)

                            # Move the file to the correct folder
                            shutil.move(src_file, os.path.join(subfolder, filename))
                            result = f"File {filename} moved to {status} folder."

                            file_logger.info("Data validation Done...")
                            db_logger.info("Data validation Done...")

                            print(f"Valid_data: {valid_data}, Invalid_data: {invalid_data}")

                        except json.JSONDecodeError as e:
                            shutil.move(src_file, os.path.join(invalid_folder, filename))
                            error = f"JSON Decode Error in file {filename}: {e}"
                            print(error)
                            invalid_data.append(filename)
                        except Exception as e:
                            shutil.move(src_file, os.path.join(invalid_folder, filename))
                            error = f"Error processing file {filename}: {e}"
                            print(error)
                            invalid_data.append(filename)
            else:
                print(f"Directory {folder_path} does not exist, skipping.")
    except Exception as e:
        print(f"An unexpected error occurred during validation {e}")

    #Transformation:
    file_logger.info("Transformation started...")
    db_logger.info("Transformation started...")

    db_config = {"host": host, "user": user, "password": password, "database": database}
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # -------User_DIMENSION_TABLE
    user_dim = Dim_User()                                     
    file_path_user = config.get('file_locations','v_location') + '/user_data/'+ 'user_data.json'
    user_source = user_dim.user_transform(file_path_user) 

    apply_scd = user_scd()
    table_name = "user_dim" 
    scd_df = apply_scd.scd_transform(user_source, table_name)
    db_logger.info("user_data successfully loaded to database")
    file_logger.info("user_data successfully loaded to database")

    # -------org_DIMENSION_TABLE
    org = Dim_Org() 
    file_path_org = config.get('file_locations','v_location') + '/org_data/'+ 'org_data.json'                                   
    org_source = org.org_transform(file_path_org)             # Transforming User data and capturing change in user_transform function

    apply_scd = org_scd()
    table_name = "organisation_dim"  # Specify your MySQL table name
    scd_df = apply_scd.scd_transform_org(org_source, table_name)
    db_logger.info("organisation_data successfully loaded to database")
    file_logger.info("organisation_data successfully loaded to database")

    # -------client_DIMENSION_TABLE
    client = Dim_Client()
    file_path_client = config.get('file_locations','v_location') + '/client_data/'+ 'client_data.json'                                      
    client_source = client.client_transform(file_path_client)           # Transforming User data and capturing change in user_transform function

    apply_scd = client_scd()
    table_name = "client_detail_dim"  # Specify your MySQL table name
    scd_df = apply_scd.scd_transform_client(client_source, table_name)
    db_logger.info("client_data successfully loaded to database")
    file_logger.info("client_data successfully loaded to database")

    # -------product_DIMENSION_TABLE
    product = Dim_product()                                      # User/user object from Dim_User Class
    file_path_prod = config.get('file_locations','v_location') + '/product_data/'+ 'product_data.json'                                   
    product_source = product.product_transform(file_path_prod)            # Transforming User data and capturing change in user_transform function

    apply_scd = product_scd()
    table_name = "product_dim"  # Specify your MySQL table name
    scd_df = apply_scd.scd_transform_prod(product_source, table_name)
    db_logger.info("product_data successfully loaded to database")
    file_logger.info("product_data successfully loaded to database")

    # -------customer_crm_DIMENSION_TABLE
    crm = Dim_crm()                                      # User/user object from Dim_User Class
    file_path_crm = config.get('file_locations','v_location') + '/crm_data/'+ 'crm_data.json'                                       
    crm_source = crm.crm_transform(file_path_crm)            # Transforming User data and capturing change in user_transform function

    apply_scd = crm_scd()
    table_name = "customer_crm_dim"  # Specify your MySQL table name
    scd_df = apply_scd.scd_transform_crm(crm_source, table_name)
    db_logger.info("crm_data successfully loaded to database")
    file_logger.info("crm_data successfully loaded to database")

    # -------channel_DIMENSION_TABLE
    channel = Dim_channel()
    file_path = config.get('file_locations','v_location') + '/channel_data/'+ 'channel_data.json'                                     
    channel_source = channel.channel_transform(file_path)           # Transforming User data and capturing change in user_transform function

    apply_scd = channel_scd()
    table_name = "channels_dim"  # Specify your MySQL table name
    scd_df = apply_scd.scd_transform_channel(channel_source, table_name)
    db_logger.info("user_data successfully loaded to database")
    file_logger.info("user_data successfully loaded to database")

    # -------FACT_TABLE(INTEGRATE CHANNELS)
    channels = Channels()
    file_path = config.get('file_locations','v_location') + '/call_data/'+ 'call_data.json'
    integrated_workforce_performance_df, integrated_interaction_df, integrated_cust_experience_df = channels.integrate(file_path)

    utils.to_database(integrated_workforce_performance_df, "workforce_performance_fact",200000)            # Loading changed data
    utils.to_database(integrated_interaction_df, "interaction_fact",200000)
    utils.to_database(integrated_cust_experience_df, "cust_experience_fact",200000)

    file_logger.info("Transformation Done...")
    db_logger.info("Transformation Done...")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    uvicorn.run("api_main:app", host="0.0.0.0", port=8000)



