import configparser
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

config = configparser.ConfigParser() 
config.read("config.ini")


class Dim_channel:
        
    def channel_transform(self,file_path):
        try: 
            spark = utils.create_session()

            
        except Exception as e:
            logging.error(f"Error in fetching channels s3_bucket details: {e}")

        # Define the channel schema for consistency
        channel_schema = StructType([
            StructField("channel_id", IntegerType(), True),
            StructField("channel_name", StringType(), True),
        ])

        # Create an empty DataFrame with the defined schema
        channel_source_df = spark.createDataFrame([], schema=channel_schema)

       
        try:
            # Read the file as a JSON into a DataFrame
                channel_df = spark.read \
                                    .option("minPartitions", 4) \
                                    .json(file_path)
                
                print("After reading JSON")
                print(f"Number of partitions before repartitioning: {channel_df.rdd.getNumPartitions()}")

                # Repartition to improve parallelism if necessary (4 is just an example)
                channel_df = channel_df.repartition(4)
                
                print("After repartitioning")
                print(f"Number of partitions after repartitioning: {channel_df.rdd.getNumPartitions()}")

                # Ensure the schema is applied by selecting and casting columns to match the channel_schema
                channel_df = channel_df.select(
                    col("channel_id"),
                    col('channel_name')
                )

                channel_source_df = channel_source_df.unionAll(channel_df)
                
        except Exception as e:
                logging.error(f'Error in fetching channel data s3_bucket: {e}')

        channel_source_df = channel_source_df.dropDuplicates(['channel_id'])
        logging.info('channel files successfully loaded from S3_bucket')

        if channel_source_df.isEmpty():
            print("No channel data available")
            logging.warning(f"No channel data available from S3_bucket")

        else:
            # channel_ids = str([row.channel_ids for row in channel_source_df.select("channel_ids").collect()])[1:-1]
            channel_ids= ",".join("'{}'".format(x[0])for x in channel_source_df.select("channel_id").rdd.collect())
            
            if len(channel_ids) > 0:
                channel_query = '(SELECT * FROM channels_dim WHERE channel_id IN ({}) AND active_flag) as query'.format(channel_ids)

                channel_target_df = spark.read.jdbc(url=url, table=channel_query, properties=mysql_properties)

                if channel_target_df.isEmpty():
                    print("Source data is totally new")
                    #print(channel_target_df.limit(10).toPandas())
                    logging.warning(f"channel Source data is totally new")

                else:
                    channel_source_col_list = channel_source_df.columns[1:]
                    channel_source_df = channel_source_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *channel_source_col_list)).cast("string"))
                    channel_target_df = channel_target_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *channel_source_col_list)).cast(
                                                                "string")).drop("channel_key_id")
                    channel_merge_df = channel_source_df.join(channel_target_df,
                                                        channel_source_df.channel_id == channel_target_df.channel_id,
                                                        "left").select(channel_source_df["*"],
                                                                    channel_target_df["channel_id"].alias("new_channel_id"),
                                                                    channel_target_df["cdc_hash"].alias("new_cdc_hash"))
                    channel_merge_df = channel_merge_df.withColumn("action", when(
                        (col("channel_id") == col("new_channel_id")) & (col("cdc_hash") != col("new_cdc_hash")),
                        "update").when((col("new_cdc_hash").isNull()) & (col("new_channel_id").isNull()),
                                    "insert").otherwise("no_action"))
                    # channel_merge_df.show()
                    channel_merge_df = channel_merge_df.filter(col("action") != "no_action")
                    # channel_merge_df.show()
                    channel_source_df = channel_merge_df.drop("cdc_hash", "new_channel_id", "new_cdc_hash", "action")
                    logging.info("channel Data is successfully fetched from S3_bucket, with CDC implemented")
        return channel_source_df
    



