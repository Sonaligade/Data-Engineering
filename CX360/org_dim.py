import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, IntegerType, StructType,StructField
from pyspark.sql.functions import col, concat_ws, sha1, when, explode
import utils
import logging

# AWS S3 bucket credentials
bucket_name = utils.bucket_name
s3 = utils.s3

# Database credentials 
url = utils.url
mysql_properties = utils.mysql_properties


class Dim_Org:
    
    def org_transform(self,file_path_org):
        
        try: 
            spark =utils.create_session()

        except Exception as e:
            logging.error("Error in get organizations file details from S3_bucket")

        org_schema = StructType([
        
            StructField("organisation_id", StringType(), True),
            StructField("organisation_name", StringType(), True),
            StructField("organisation_email", StringType(), True),
            StructField("organisation_mobile", StringType(), True),
            StructField("organisation_address", StringType(), True),
            StructField("organisation_city", StringType(), True),
            StructField("organisation_state", StringType(), True),
            StructField("organisation_country", StringType(), True),
            StructField("organisation_pincode", StringType(), True)
            
        ])

        org_source_df = spark.createDataFrame([], schema=org_schema)

        
        try:
                
            df = spark.read \
            .option("minPartitions", 4) \
            .option("mode","PERMISSIVE")\
            .json(file_path_org)
            print("after reading")
            print(df.rdd.getNumPartitions())
            df = df.repartition(4)
            print("after repartitioning")
            print(df.rdd.getNumPartitions())

            df = df.dropna() 
            # print("----- check drop null----------")
            # df.show(100,truncate=False)
            
            #creating organization table

            org_df = df.select(col("organisation_id"), col("organisation_name"), col("organisation_email"), col("organisation_mobile"), \
                            col("organisation_address"), col("organisation_city"), col("organisation_state"), col("organisation_country"), \
                            col("organisation_pincode")) 
                           


            org_source_df = org_source_df.unionAll(org_df)

                
        except Exception as e:
                logging.error(f'Error in fetching organization file from S3_bucket: {e}')

        org_source_df = org_source_df.dropDuplicates(['organisation_id'])
        logging.info("Organization source file successfully fetched ")

        if org_source_df.isEmpty():
            print("No Organisation data available")
            logging.warning("No Organization data available")
        else:
        # capturing the change for Organization
            # org_ids = str([row.OrganizationID for row in org_source_df.select("OrganizationID").collect()])[1:-1]
            org_ids= ",".join("'{}'".format(x[0]) for x in org_source_df.select("organisation_id").rdd.collect())
            
            if len(org_ids) > 0:
                org_query = '(SELECT * FROM organisation_dim WHERE organisation_id IN ({}) AND active_flag) as query'.format(
                    org_ids)
                org_target_df = spark.read.jdbc(url=url, table=org_query, properties=mysql_properties)
                
                if org_target_df.isEmpty():
                    print("Source data is totally new")
                    logging.warning("Organization data is totally new")
                
                else:
                    org_source_col_list = org_source_df.columns[1:]
                    org_source_df = org_source_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *org_source_col_list)).cast("string"))
                    org_target_df = org_target_df.withColumn("cdc_hash",
                                                            sha1(concat_ws("||", *org_source_col_list)).cast(
                                                                "string")).drop("org_key_id")

                    org_merge_df = org_source_df.join(org_target_df,
                                                        org_source_df.organisation_id == org_target_df.organisation_id,
                                                        "left").select(org_source_df["*"],
                                                                    org_target_df["organisation_id"].alias("new_organisation_id"),
                                                                    org_target_df["cdc_hash"].alias("new_cdc_hash"))
                    org_merge_df = org_merge_df.withColumn("action", when(
                        (col("organisation_id") == col("new_organisation_id")) & (col("cdc_hash") != col("new_cdc_hash")),
                        "update").when((col("new_cdc_hash").isNull()) & (col("new_organisation_id").isNull()),
                                    "insert").otherwise("no_action"))
                    # cust_merge_df.show()
                    org_merge_df = org_merge_df.filter(col("action") != "no_action")
                    #org_merge_df.show()
                    org_source_df = org_merge_df.drop("cdc_hash", "new_organisation_id", "new_cdc_hash", "action")
                    logging.info("Organization Data is successfully fetched from S3_bucket, with CDC implemented")

        
        
        return org_source_df
    

