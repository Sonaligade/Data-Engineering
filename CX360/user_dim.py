import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, StructType,StructField,BooleanType
from pyspark.sql.functions import col, concat_ws,sha1,when,lit
import utils
import logging
# from src.exception import CustomException
import sys

bucket_name = utils.bucket_name
s3 = utils.s3
url = utils.url
mysql_properties = utils.mysql_properties


class Dim_User:
        
    def user_transform(self,file_path_user):
        try: 
            spark = utils.create_session()
        
        except Exception as e:
            logging.error(f"Error in fetching users s3_bucket details: {e}")
        
        user_schema = StructType([
            StructField("user_id",  LongType(), True),  
            StructField("role_name", StringType(), True), 
            StructField("first_name",StringType(), True),
            StructField("last_name",StringType(), True), 
            StructField("email", StringType(), True),
            StructField("role_id", LongType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("date_joined", StringType(), True),
            StructField("user_email" ,StringType(), True),
            StructField("organisation_id", LongType(), True), 
            StructField("customer_id", LongType(), True), 
            StructField("user_processes", StringType(), True), 
            StructField("empcode", StringType(), True),  
            StructField("user_mobile", StringType(), True),  
            StructField("tenure", StringType(), True),  
            StructField("bucket", StringType(), True),  
            StructField("status",StringType(), True),  
        ])
        user_source_df = spark.createDataFrame([], schema=user_schema)

        try:

            user_df = spark.read \
                            .option("minPartitions", 4) \
                            .option("mode","PERMISSIVE")\
                            .json(file_path_user)
           
            print("after reading")
            print(user_df.rdd.getNumPartitions())
            user_df = user_df.repartition(4)
            print("after repartitioning")
            print(user_df.rdd.getNumPartitions())
            
           
            user_df = user_df.select(
                                    col("user_id"), col("role_name"),col("first_name"),
                                    col("last_name"), col("email"),col("role_id"),
                                    col("is_active").cast(BooleanType()),col("date_joined"),col("user_email"),
                                    col("organisation_id"),col("customer_id"), col("processes").alias("user_processes"),
                                    col("empcode"),col("user_mobile"),col("tenure"),col("bucket"),col("status")
                                    )
            user_source_df = user_source_df.unionByName(user_df, allowMissingColumns=True)

        except Exception as e:
                logging.error(f'Error in fetching user data s3_bucket: {e}')

        user_source_df = user_source_df.dropDuplicates(['user_id'])
        logging.info('user files successfully loaded from S3_bucket')

        if user_source_df.isEmpty():
            print("No user data available")
            logging.warning(f"No user data available from S3_bucket")

        else:
            # user_ids = str([row.user_id for row in user_source_df.select("user_id").collect()])[1:-1]
            user_ids= ",".join("'{}'".format(x[0])for x in user_source_df.select("user_id").rdd.collect())
            
            if len(user_ids) > 0:
                user_query = '(SELECT * FROM user_dim WHERE user_id IN ({}) AND active_flag) as query'.format(
                    user_ids)
                user_target_df = spark.read.jdbc(url=url, table=user_query, properties=mysql_properties)
                
                if user_target_df.isEmpty():
                    print("Source data is totally new")
                    logging.warning(f"user Source data is totally new")

                else:
                    user_source_col_list = user_source_df.columns[1:]
                    user_source_df = user_source_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *user_source_col_list)).cast("string"))
                    user_target_df = user_target_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *user_source_col_list)).cast(
                                                                "string")).drop("user_key_id")
                    user_merge_df = user_source_df.join(user_target_df,
                                                        user_source_df.user_id == user_target_df.user_id,
                                                        "left").select(user_source_df["*"],
                                                                    user_target_df["user_id"].alias("new_user_id"),
                                                                    user_target_df["cdc_hash"].alias("new_cdc_hash"))
                    user_merge_df = user_merge_df.withColumn("action", when(
                        (col("user_id") == col("new_user_id")) & (col("cdc_hash") != col("new_cdc_hash")),
                        "update").when((col("new_cdc_hash").isNull()) & (col("new_user_id").isNull()),
                                    "insert").otherwise("no_action"))
                    # user_merge_df.show()
                    user_merge_df = user_merge_df.filter(col("action") != "no_action")
                    # user_merge_df.show(50,truncate=False)
                    user_source_df = user_merge_df.drop("cdc_hash", "new_user_id", "new_cdc_hash", "action")
                    logging.info("user Data is successfully fetched from S3_bucket, with CDC implemented")

            

        return user_source_df
    