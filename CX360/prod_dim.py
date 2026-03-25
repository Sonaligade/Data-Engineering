import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, StructType, StructField, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, concat_ws, sha1, when
import utils
import logging

import sys

bucket_name = utils.bucket_name
s3 = utils.s3
url = utils.url
mysql_properties = utils.mysql_properties


class Dim_product:
    def product_transform(self, file_path_prod):

        try: 
            spark = utils.create_session()

           
        except Exception as e:
            logging.error(f'Error in fetching product s3_bucket details: {e}')

        # defining the empty schema for validation 
        prod_schema = StructType([
            StructField('product_id', LongType(), True),
            StructField('organisation_id', LongType(), True),
            StructField('product_name', StringType(), True),
            StructField('category', StringType(), True),
            StructField('description', StringType(), True),
            StructField('owner', StringType(), True),
            StructField('price', StringType(), True)
        ])

        prod_source_df = spark.createDataFrame([], schema = prod_schema)

        try:
            prod_df = spark.read\
                                .option('minPartitions', 4) \
                                .json(file_path_prod)
            print("After reading JSON")
            print(f"Number of partitions before repartitioning: {prod_df.rdd.getNumPartitions()}")

            # Repartition to improve parallelism if necessary (4 is just an example)
            prod_df = prod_df.repartition(4)
            
            print("After repartitioning")
            print(f"Number of partitions after repartitioning: {prod_df.rdd.getNumPartitions()}")

            prod_df = prod_df.select(
                col('product_id'),
                col('organisation_id'),
                col('product_name'),
                col('category'),
                col('description'),
                col('owner'),
                col('price')
            )

            prod_source_df = prod_source_df.unionAll(prod_df)
        
        except Exception as e:
                logging.error(f'Error in fetching product data from s3 bucket: {e}')
        
        prod_source_df = prod_source_df.dropDuplicates(['product_id'])
        logging.info('product files successfully loaded from s3 bucket')

        if prod_source_df.isEmpty():
            print("No product data available")
            logging.warning(f"No product data available from S3_bucket")

        else:
            prod_ids = ",".join("'{}'".format(x[0])for x in prod_source_df.select("product_id").rdd.collect())
            
            if len(prod_ids) > 0:
                prod_query = '(SELECT * FROM product_dim WHERE product_id IN ({}) AND active_flag) as query'.format(prod_ids)

                prod_target_df = spark.read.jdbc(url=url, table=prod_query, properties=mysql_properties)

                if prod_target_df.isEmpty():
                    print("Source data is totally new")
                    # print(prod_target_df.limit(10).toPandas())
                    logging.warning(f"product Source data is totally new")

                else:
                    prod_source_col_list = prod_source_df.columns[1:]
                    prod_source_df = prod_source_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *prod_source_col_list)).cast("string"))
                    prod_target_df = prod_target_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *prod_source_col_list)).cast(
                                                                "string")).drop("product_key_id")
                    prod_merge_df = prod_source_df.join(prod_target_df,
                                                        prod_source_df.product_id == prod_target_df.product_id,
                                                        "left").select(prod_source_df["*"],
                                                                    prod_target_df["product_id"].alias("new_product_id"),
                                                                    prod_target_df["cdc_hash"].alias("new_cdc_hash"))
                    prod_merge_df = prod_merge_df.withColumn("action", when(
                        (col("product_id") == col("new_product_id")) & (col("cdc_hash") != col("new_cdc_hash")),
                        "update").when((col("new_cdc_hash").isNull()) & (col("new_product_id").isNull()),
                                    "insert").otherwise("no_action"))
                    # prod_merge_df.show()
                    prod_merge_df = prod_merge_df.filter(col("action") != "no_action")
                    # prod_merge_df.show()
                    prod_source_df = prod_merge_df.drop("cdc_hash", "new_product_id", "new_cdc_hash", "action")
                    logging.info("product Data is successfully fetched from S3_bucket, with CDC implemented")
        return prod_source_df
    

               