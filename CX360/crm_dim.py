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


bucket_name = utils.bucket_name
s3 = utils.s3
url = utils.url
mysql_properties = utils.mysql_properties


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

        customer_source_df = customer_source_df.dropDuplicates(['customer_id'])
        logging.info('customer files successfully loaded from S3_bucket')

        if customer_source_df.isEmpty():
            print("No customer data available")
            logging.warning(f"No customer data available from S3_bucket")

        else:
            # customer_ids = str([row.customer_ids for row in customer_source_df.select("customer_ids").collect()])[1:-1]
            customer_ids= ",".join("'{}'".format(x[0])for x in customer_source_df.select("customer_id").rdd.collect())
            
            if len(customer_ids) > 0:
                customer_query = '(SELECT * FROM customer_crm_dim WHERE customer_id IN ({}) AND active_flag) as query'.format(customer_ids)

                customer_target_df = spark.read.jdbc(url=url, table=customer_query, properties=mysql_properties)

                if customer_target_df.isEmpty():
                    print("Source data is totally new")
                    #print(customer_target_df.limit(10).toPandas())
                    logging.warning(f"customer Source data is totally new")

                else:
                    customer_source_col_list = customer_source_df.columns[1:]
                    customer_source_df = customer_source_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *customer_source_col_list)).cast("string"))
                    customer_target_df = customer_target_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *customer_source_col_list)).cast(
                                                                "string")).drop("customer_key_id")
                    customer_merge_df = customer_source_df.join(customer_target_df,
                                                        customer_source_df.customer_id == customer_target_df.customer_id,
                                                        "left").select(customer_source_df["*"],
                                                                    customer_target_df["customer_id"].alias("new_customer_id"),
                                                                    customer_target_df["cdc_hash"].alias("new_cdc_hash"))
                    customer_merge_df = customer_merge_df.withColumn("action", when(
                        (col("customer_id") == col("new_customer_id")) & (col("cdc_hash") != col("new_cdc_hash")),
                        "update").when((col("new_cdc_hash").isNull()) & (col("new_customer_id").isNull()),
                                    "insert").otherwise("no_action"))
                    # customer_merge_df.show()
                    customer_merge_df = customer_merge_df.filter(col("action") != "no_action")
                    # customer_merge_df.show()
                    customer_source_df = customer_merge_df.drop("cdc_hash", "new_customer_id", "new_cdc_hash", "action")
                    logging.info("customer Data is successfully fetched from S3_bucket, with CDC implemented")
        return customer_source_df
    

