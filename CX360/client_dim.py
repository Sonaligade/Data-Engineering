import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,StructField,StructType
from pyspark.sql.functions import col, concat_ws,sha1,when,lit
import utils
import logging
# from src.exception import CustomException
import sys


bucket_name = utils.bucket_name
s3 = utils.s3
url = utils.url
mysql_properties = utils.mysql_properties


class Dim_Client:
        
    def client_transform(self,file_path_client):
        try: 
            spark = utils.create_session()
        
        except Exception as e:
            logging.error(f"Error in fetching agents s3_bucket details: {e}")
        
        client_schema = StructType([
            StructField("client_id",  IntegerType(), True),  
            StructField("organisation_id", IntegerType(), True), 
            StructField("client_first_name",StringType(), True),
            StructField("client_last_name",StringType(), True), 
            StructField("client_language", StringType(), True),
            StructField("client_email", StringType(), True),
            StructField("client_mobile", StringType(), True)
        ])

        client_source_df = spark.createDataFrame([], schema = client_schema)

        try:

            client_df = spark.read \
                            .option("minPartitions", 4) \
                            .option("mode","PERMISSIVE")\
                            .json(file_path_client)
            
            print("after reading")
            print(client_df.rdd.getNumPartitions())
            client_df = client_df.repartition(4)
            print("after repartitioning")
            print(client_df.rdd.getNumPartitions())

            client_df = client_df.select(
                                    col("client_id"), col("organisation_id"),col("client_first_name"),
                                    col("client_last_name"), col("client_language"),col("client_email"),
                                    col("client_mobile")
                                    )
            client_source_df = client_source_df.unionByName(client_df, allowMissingColumns=True)

        except Exception as e:
                logging.error(f'Error in fetching agent data s3_bucket: {e}')

        logging.info('client files successfully loaded from S3_bucket')

        if client_source_df.isEmpty():
            print("No Client data available")
            logging.warning(f"No Client data available from S3_bucket")

        else:
            client_ids= ",".join("'{}'".format(x[0])for x in client_source_df.select("client_id").rdd.collect())
             
            if len(client_ids) > 0:
                client_query = '(SELECT * FROM client_detail_dim WHERE client_id IN ({}) AND active_flag) as query'.format(
                    client_ids)
                client_target_df = spark.read.jdbc(url=url, table=client_query, properties=mysql_properties)
                
                if client_target_df.isEmpty():
                    print("Source data is totally new")

                    client_source_df = client_source_df.dropDuplicates(['client_id'])
                    logging.warning(f"client Source data is totally new")

                else:
                    client_source_col_list = client_source_df.columns[1:]
                    client_source_df = client_source_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *client_source_col_list)).cast("string"))
                    client_target_df = client_target_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *client_source_col_list)).cast("string")).drop("user_key_id")
                    client_merge_df = client_source_df.join(client_target_df,
                                                        client_source_df.client_id == client_target_df.client_id,
                                                        "left").select(client_source_df["*"],
                                                                    client_target_df["client_id"].alias("new_client_id"),
                                                                    client_target_df["cdc_hash"].alias("new_cdc_hash"))
                    client_merge_df = client_merge_df.withColumn("action", when(
                        (col("client_id") == col("new_client_id")) & (col("cdc_hash") != col("new_cdc_hash")),
                        "update").when((col("new_cdc_hash").isNull()) & (col("new_client_id").isNull()),
                                    "insert").otherwise("no_action"))
                    client_merge_df = client_merge_df.filter(col("action") != "no_action")
                    client_source_df = client_merge_df.drop("cdc_hash", "new_client_id", "new_cdc_hash", "action")
                    logging.info("client Data is successfully fetched from S3_bucket, with CDC implemented")

            print(client_source_df.show(50,truncate=False))

        return client_source_df
    