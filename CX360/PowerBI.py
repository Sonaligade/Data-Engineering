from fastapi import FastAPI, UploadFile, HTTPException, Form, File, Request, Depends
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, StructType,StructField, IntegerType, DoubleType, DateType 
from pyspark.sql.functions import col, concat_ws,sha1,when
import utils
import logging
# from src.exception import CustomException
import sys
import uvicorn
app = FastAPI()

bucket_name = utils.bucket_name
s3 = utils.s3
url = utils.url
mysql_properties = utils.mysql_properties

@app.post("/load_crm_data/")

class Dim_crm:
        
    def crm_transform(self,file_path_crm):
        try: 
            spark = utils.create_session()

        except Exception as e:
            logging.error(f"Error in fetching customers s3_bucket details: {e}")

        # Define the customer schema for consistency
        customer_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("DOB", DateType(), True),
            StructField("gender", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("customer_income", DoubleType(), True),
            StructField("preferred_language", StringType(), True),
            StructField("total_spent", DoubleType(), True),
            StructField("social_profile", StringType(), True),
            StructField("customer_since", StringType(), True),
            StructField("customer_status", StringType(), True),
            StructField("customer_churn", StringType(), True),
        ])

        # Create an empty DataFrame with the defined schema
        customer_source_df = spark.createDataFrame([], schema=customer_schema)

        
        try:
            # Read the file as a JSON into a DataFrame
            customer_df = spark.read \
                                .option("minPartitions", 4) \
                                .json(file_path_crm)
            
            print("After reading JSON")
            print(f"Number of partitions before repartitioning: {customer_df.rdd.getNumPartitions()}")

            # Repartition to improve parallelism if necessary (4 is just an example)
            customer_df = customer_df.repartition(4)
            
            print("After repartitioning")
            print(f"Number of partitions after repartitioning: {customer_df.rdd.getNumPartitions()}")

            # Ensure the schema is applied by selecting and casting columns to match the customer_schema
            customer_df = customer_df.select(
                col("customer_id").cast(IntegerType()),
                col("first_name"),
                col("last_name"),
                col("email"),
                col("phone_number"),
                col("DOB").cast(DateType()),
                col("gender"),
                col("address"),
                col("city"),
                col("state"),
                col("country"),
                col("postal_code"),
                col("customer_income").cast(DoubleType()),
                col("preferred_language"),
                col("total_spent").cast(DoubleType()),
                col("social_profile"),
                col("customer_since"),
                col("customer_status"),
                col("customer_churn")
            )

            customer_source_df = customer_source_df.unionAll(customer_df)
                
        except Exception as e:
            logging.error(f'Error in fetching customer data s3_bucket: {e}')
        
        return customer_source_df
    

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
