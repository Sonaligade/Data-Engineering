import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, StructType, StructField, DateType, TimestampType, FloatType, BooleanType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import concat_ws,col, lit, udf, to_json,date_format, expr, format_string, when, array, coalesce
import datetime
import utils
from pyspark.sql import functions as F
from fastapi import HTTPException
from pyspark.sql.functions import regexp_replace
import logging
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, MapType ,FloatType
import os
from logger import log_audit_event, setup_audit_logging, setup_file_logger, setup_db_logging, file_logger, db_logger

# Initialize loggers
file_logger = setup_file_logger()
db_logger = setup_db_logging()
audit_logger = setup_audit_logging()

file_logger = logging.getLogger('fileLogger')
db_logger = logging.getLogger('dblogger')
audit_logger = logging.getLogger('auditLogger')

bucket_name = utils.bucket_name
s3 = utils.s3
url = utils.url
mysql_properties = utils.mysql_properties

class Channels:
    
    def call_transform(self, file_path_call):
        
        spark =utils.create_session()       
        workforce_performance_fact_schema = StructType([
            StructField("interaction_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("organisation_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("channel_id", IntegerType(), True),
            StructField("processes", StringType(), True),
            StructField("agent_final_tone", StringType(), True),
            StructField("agent_keywords", StructType(), True),
            StructField("agent_sentiments", StringType(), True),
            StructField("average_compliance_score", FloatType(), True),
            StructField("ai_feedback", StringType(), True),
            StructField("ai_feedback_reason", StringType(), True),
            StructField("manual_feedback", StringType(), True),
            StructField("manual_rating", LongType(), True),
            StructField("agent_transcript",StringType() , True)
        ])

        new_workforce_performance_fact_df = spark.createDataFrame([], schema=workforce_performance_fact_schema)         # creating the empty dataframe with defined schema

        interaction_fact_schema = StructType([
            StructField("interaction_id", IntegerType(), True),
            StructField("interaction_file_id", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("organisation_id", IntegerType(), True),
            StructField("client_id", IntegerType(), True),
            StructField("channel_id", IntegerType(), True),
            StructField("department", StringType(), True),
            StructField("wait_time", StringType(), True),
            StructField("hold_time", StringType(), True),
            StructField("dead_air", StringType(), True),
            StructField("interaction_duration", StringType(), True),
            StructField("interaction_date", TimestampType(), True),
            StructField("interaction_status", StringType(), True),
            StructField("interaction_transferred", StringType(), True),
            StructField("customer_intent", StringType(), True),
            StructField("self_service", StringType(), True),
            StructField("full_transcript", StringType(), True),
            StructField("escalation", StringType(), True),
            StructField("escalation_reason", StringType(), True),
            StructField("language_detection", StringType(), True),
            StructField("interaction_channel", StringType(), True),
        ])

        new_interaction_fact_df = spark.createDataFrame([], schema=interaction_fact_schema)

        cust_experience_fact_schema = StructType([
            StructField("interaction_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("organisation_id", IntegerType(), True),
            StructField("channel_id", IntegerType(), True),
            StructField("csat_score", LongType(), True),
            StructField("ces_score", LongType(), True),
            StructField("nps_score", LongType(), True),
            StructField("customer_rating", StringType(), True),
            StructField("customer_intent", StringType(), True),
            StructField("customer_final_tone", StringType(), True),
            StructField("customer_keyword", StructType(), True),
            StructField("customer_sentiment", StringType(), True),
            StructField("customer_transcript", StringType(), True)
        ])

        new_cust_experience_fact_df = spark.createDataFrame([], schema=cust_experience_fact_schema)

        try:

                call_df = spark.read \
                                .option("mode","PERMISSIVE")\
                                .option("minPartitions", 4) \
                                .json(file_path_call)
                
                call_df.printSchema()
                # print(call_df.rdd.getNumPartitions())
                call_df = call_df.repartition(4)
                # print(call_df.rdd.getNumPartitions())

                customer_intent_keys = [field.name for field in call_df.schema["customer_intent"].dataType.fields]

                print(f"count of records : {call_df.rdd.count()}")
                
                flattened_workforce_performance_fact_df = call_df.select(
                    col("call_id").alias("interaction_id"), 
                    col("agent_id").alias("user_id"), 
                    col("organisation_id"),
                    col("product_id"), 
                    col("channel_id").cast(IntegerType()),
                    regexp_replace(col("processes"), r'\[|\]', '').alias("processes"), 
                    col("call_data.agent_final_tone").alias("agent_final_tone"),
                    (col("call_data.keywords_tracking.agent_keywords_tracking")).alias(
                        "agent_keywords"),
                    to_json(col("call_data.Sentiment.agent_sentiment")).alias("agent_sentiments"),
                    col("ai_feedback"),
                    col("ai_feedback_reason"),
                    col("manual_feedback"),
                    col("average_compliance_score"),
                    col("manual_rating").cast(IntegerType()),
                    concat_ws("", 
                            col("call_data.transcript.agent.hindi").isNotNull(),(col("call_data.transcript.agent.hindi")),   # Convert Hindi array to JSON string
                            col("call_data.transcript.agent.english").isNotNull(),(col("call_data.transcript.agent.english")),  # Convert English array to JSON string
                            col("call_data.transcript.agent.arabic").isNotNull(),(col("call_data.transcript.agent.arabic")),   # Convert Arabic array to JSON string
                            col("call_data.transcript.agent.marathi").isNotNull(),(col("call_data.transcript.agent.marathi")),   # Convert Marathi array to JSON string
                            col("call_data.transcript.agent.English").isNotNull(),(col("call_data.transcript.agent.English"))
                        ).alias("agent_transcript")
                )
                flattened_workforce_performance_fact_df = flattened_workforce_performance_fact_df.withColumn("agent_transcript",regexp_replace("agent_transcript", r'\[|\]', ''))
                                                    
              
                print("printing flattened_workforce_performance_fact_df df schema")
                print(flattened_workforce_performance_fact_df.printSchema())
                new_workforce_performance_fact_df = new_workforce_performance_fact_df.unionByName(flattened_workforce_performance_fact_df, allowMissingColumns=True)
                print("printing new_workforce_performance_fact_df df schema")
                print(new_workforce_performance_fact_df.printSchema())
               
                flattened_interaction_fact_df = call_df.select(
                    col("call_id").alias("interaction_id"), 
                    col("interaction_file_id"),
                    col("customer_id"),
                    col("agent_id").alias("user_id"),  
                    col("product_id"), 
                    col("organisation_id"),
                    col("client_id"),
                    col("channel_id").cast(IntegerType()),
                    col("department"),
                    col("call_data.wait_time"),
                    col("call_data.hold_time"),
                    col("dead_air"),
                    col("interaction_duration").cast(StringType()),
                    col("interaction_date").cast(TimestampType()),
                    col("call_data.call_status").alias("interaction_status"),
                    col("call_data.transfered_call").alias("interaction_transferred"),
                    lit(", ".join(customer_intent_keys)).alias("customer_intent"),
                    col("self_service"),
                    concat_ws("", 
                            (col("call_data.transcript.full.hindi")),   # Convert Hindi array to JSON string
                            (col("call_data.transcript.full.english")),  # Convert English array to JSON string
                            (col("call_data.transcript.full.arabic")),   # Convert Arabic array to JSON string
                            (col("call_data.transcript.full.marathi")),   # Convert Marathi array to JSON string
                            (col("call_data.transcript.full.English"))
                        ).alias("full_transcript"),
                    col("escalation"), 
                    col("escalation_reason"),
                    col("call_data.language_detection").cast("string") 
                )
                
                flattened_interaction_fact_df = flattened_interaction_fact_df.withColumn("interaction_channel", lit("call").cast("string"))
                new_interaction_fact_df = new_interaction_fact_df.unionByName(flattened_interaction_fact_df,allowMissingColumns=True)
                print("printing new_interaction_fact_df df schema")
                print(new_interaction_fact_df.printSchema())


                flattened_cust_experience_fact_df = call_df.select(
                    col("call_id").alias("interaction_id"),
                    col("customer_id"),
                    col("product_id"), 
                    col("organisation_id"),
                    col("channel_id"),
                    col("call_data.customer_satisfaction_rates").cast(LongType()).alias("csat_score"),
                    col("call_data.customer_effort_score").cast(LongType()).alias("ces_score"),
                    col("call_data.NPS_Score").cast(LongType()).alias("nps_score"),
                    concat_ws("", 
                        (col("call_data.transcript.customer.hindi")),   # Convert Hindi array to JSON string
                        (col("call_data.transcript.customer.english")),  # Convert English array to JSON string
                        (col("call_data.transcript.customer.arabic")),   # Convert Arabic array to JSON string
                        (col("call_data.transcript.customer.marathi")),   # Convert Marathi array to JSON string
                        (col("call_data.transcript.customer.English"))
                    ).alias("customer_transcript"),
                    col("customer_rating"),
                    lit(", ".join(customer_intent_keys)).alias("customer_intent"),
                    col("call_data.cust_final_tone").alias("customer_final_tone"),
                    (col("call_data.keywords_tracking.customer_keywords_tracking")).alias(
                        "customer_keyword"),
                    to_json(col("call_data.Sentiment.customer_sentiment")).alias("customer_sentiment")
                )
                flattened_cust_experience_fact_df = flattened_cust_experience_fact_df.withColumn("customer_transcript", regexp_replace("customer_transcript", r'\[|\]', ''))
                
                print("printing flattened_cust_experience_fact_df df schema")
                print(flattened_cust_experience_fact_df.printSchema())
                new_cust_experience_fact_df = new_cust_experience_fact_df.unionByName(flattened_cust_experience_fact_df,allowMissingColumns=True)

                print(set(flattened_cust_experience_fact_df.columns) == set(new_cust_experience_fact_df.columns))

                print("printing new_cust_experience_fact_df df schema")
                print(new_cust_experience_fact_df.printSchema())

        except Exception as e:
                logging.error(f"Error in call data loading and dataframe creation: {e}")
        
        # print(new_workforce_performance_fact_df.printSchema())
        # print(new_interaction_fact_df.printSchema())
        # print(new_cust_experience_fact_df.printSchema())
        new_workforce_performance_fact_df = new_workforce_performance_fact_df.dropDuplicates(['interaction_id'])
        # new_workforce_performance_fact_df.show(5)
        # new_interaction_fact_df = new_interaction_fact_df.dropDuplicates(['interaction_id'])
        # new_cust_experience_fact_df = new_cust_experience_fact_df.dropDuplicates(['interaction_id'])
        db_logger.info("Call File transformed successfully")
        return new_workforce_performance_fact_df, new_interaction_fact_df, new_cust_experience_fact_df
    
    def integrate(self, org_name):
        spark = utils.create_session()
    # Collecting the three DataFrames
        try:
            new_workforce_performance_fact_df, new_interaction_fact_df, new_cust_experience_fact_df= self.call_transform(org_name)
              
        except Exception as e:
            file_logger.error(f"Error in call_transform: {e}")
            raise HTTPException(status_code=500, detail="Error during data transformation.")

        try:
            
            get_max_value = udf(utils.get_max_value, StringType())
            flatten_array_of_structs= udf(utils.flatten_array_of_structs, StringType())

            # FINDING MAX SENTIMENT AND SENTIMENT SCORE for agent-----------------------------------------------------------
            
                        # implementing the udf function to get the maximum sentiment and its score and  create the structured columns
            integrated_df = new_workforce_performance_fact_df.withColumn("agent_sentiment_value", get_max_value(new_workforce_performance_fact_df.agent_sentiments)).drop("agent_sentiments") \
                                                            .withColumn("agent_keyword", flatten_array_of_structs(new_workforce_performance_fact_df.agent_keywords)).drop("agent_keywords") 
            
                        #now The new sentiment value columns are further processed by extracting substrings and splitting them into separate columns for sentiment and sentiment score.
            integrated_df = integrated_df.withColumn("string_column_1", expr("substring(agent_sentiment_value, 2, length(agent_sentiment_value) - 2)")) \
                                                    
            integrated_df = integrated_df.withColumn("agent_sentiment", expr("split(string_column_1, ',')[0]")) \
                                                    .withColumn("agent_sentiment_score", expr("split(string_column_1, ',')[1]"))
                        
            integrated_df = integrated_df.drop("string_column_1", "string_column_2", "agent_sentiment_value")
            integrated_workforce_performance_df = integrated_df.select("interaction_id", "user_id","organisation_id", "product_id", "channel_id", "processes", 
                                                                    "agent_transcript", "agent_final_tone","agent_keyword", "agent_sentiment","agent_sentiment_score", 
                                                                    "average_compliance_score", "ai_feedback", "ai_feedback_reason", "manual_feedback", "manual_rating")
            
                        
            integrated_interaction_df = new_interaction_fact_df.select("interaction_id", "interaction_file_id", "customer_id", "user_id", "product_id", 
                                                                               "organisation_id", "client_id", "channel_id", "department", "wait_time", "hold_time", "dead_air", "interaction_duration", "interaction_date", 
                                                                               "interaction_status", "interaction_transferred", "self_service", "full_transcript", "escalation", "escalation_reason","customer_intent")
                            
            # FINDING MAX SENTIMENT AND SENTIMENT SCORE for customer-------------------------------------------------------------
            
                        # implementing the udf function to get the maximum sentiment and its score and  create the structured columns
            integrated_df = new_cust_experience_fact_df.withColumn("customer_sentiment_value", get_max_value(new_cust_experience_fact_df.customer_sentiment)).drop("customer_sentiment") \
                                                       .withColumn("customer_keywords", flatten_array_of_structs(new_cust_experience_fact_df.customer_keyword)).drop("customer_keyword") 
                        #now The new sentiment value columns are further processed by extracting substrings and splitting them into separate columns for sentiment and sentiment score.
            integrated_df = integrated_df.withColumn("string_column_1", expr("substring(customer_sentiment_value, 2, length(customer_sentiment_value) - 2)")) \
                                                    
            integrated_df = integrated_df.withColumn("customer_sentiment", expr("split(string_column_1, ',')[0]")) \
                                                    .withColumn("customer_sentiment_score", expr("split(string_column_1, ',')[1]"))
                                                    
            integrated_df = integrated_df.withColumn("customer_sentiment_value", expr("split(string_column_1, ',')[0]")) \
                                                    .withColumn("customer_sentiment_score", expr("split(string_column_1, ',')[1]"))
                        
            integrated_df = integrated_df.drop("string_column_1", "string_column_2", "customer_sentiment_value")

            integrated_cust_experience_df = integrated_df.select("interaction_id", 
                                                                       "customer_id", "product_id", "organisation_id", "channel_id", "csat_score", "ces_score", 
                                                                                "nps_score", "customer_transcript", "customer_rating", "customer_intent",
                                                                                        "customer_final_tone", "customer_keywords", "customer_sentiment", "customer_sentiment_score"
                                                                                        )

            print(integrated_workforce_performance_df.printSchema())
            print(integrated_interaction_df.printSchema())
            print(integrated_cust_experience_df.printSchema())
        except Exception as e:
            file_logger.error(f"Error in transforming call channel data: {e}")    

        
# adding surrogate keys to integrated_workforce_performance_fact_df
        # ADDING SURROGATE KEY OF DIMENSION TABLES as foreign key to integrated df
        integration_ids =", ".join("'{}'".format(x[0]) for x in integrated_workforce_performance_df.select("interaction_id").rdd.collect())
        if len(integration_ids) > 0:
            query = '(SELECT * FROM workforce_performance_fact WHERE interaction_id IN ({})) as query'.format(integration_ids)
            df = spark.read.jdbc(url=url, table=query, properties=mysql_properties)
            ids = str([row.interaction_id for row in df.select("interaction_id").collect()])[1:-1]

            if df.isEmpty():
                print("New data is going to load in Database")
            else:
                logging.warning(f"The incoming data from source is already present in workforce_performance_fact table for following  interaction_ids :{ids}")
                raise HTTPException(status_code=400,
                                    detail=f"The incoming data from source is already present in workforce_performance_fact table for following  interaction_ids :{ids}")

        # print(integrated_df.show(2))
        # print(integrated_df.printSchema())
        
        # Adding surrogate keys for agent
        user_ids = ",".join("'{}'".format(x[0]) for x in integrated_workforce_performance_df.select("user_id").rdd.collect())
        if len(user_ids) > 0:
            agent_query = '(SELECT * FROM user_dim WHERE user_id IN ({}) AND active_flag) as query'.format(user_ids)
            agent_temp_df = spark.read.jdbc(url=url, table=agent_query, properties=mysql_properties)
            
            if agent_temp_df.isEmpty():
                logging.warning(f"Agent information isn't present. Please load agent data in database")
                raise HTTPException(status_code=400, detail=f"Agent information isn't present. Please load agent data in database")

            agent_int_df = integrated_workforce_performance_df.join(agent_temp_df, integrated_workforce_performance_df.user_id == agent_temp_df.user_id,"left")\
                                        .select(integrated_workforce_performance_df["*"], agent_temp_df["user_key_id"].alias("new_ID"))
            
            integrated_workforce_performance_df = agent_int_df.drop("user_id").withColumnRenamed("new_ID", "user_id")


        # Adding surrogate keys for org
        org_IDs= ",".join("'{}'".format(x[0]) for x in integrated_workforce_performance_df.select("organisation_id").rdd.collect())
        if len(org_IDs) > 0:
            org_query = '(SELECT * FROM organisation_dim WHERE organisation_id IN ({}) AND active_flag) as query'.format(org_IDs)
            org_temp_df = spark.read.jdbc(url=url, table=org_query, properties=mysql_properties)
            
            if org_temp_df.isEmpty():
                logging.warning(f"organisation information isn't present. Please load organisation data in database")
                raise HTTPException(status_code=400, detail=f"organisation information isn't present. Please load organisation data in database")

            org_int_df = integrated_workforce_performance_df.join(org_temp_df, integrated_workforce_performance_df.organisation_id == org_temp_df.organisation_id, "left")\
                                    .select(integrated_workforce_performance_df["*"], org_temp_df["org_key_id"].alias("new_ID"))
            
            integrated_workforce_performance_df = org_int_df.drop("organisation_id").withColumnRenamed("new_ID", "organisation_id")
        
        # Adding surrogate keys for product
        pdt_IDs = ','.join("'{}'".format(x[0]) for x in integrated_workforce_performance_df.select("product_id").rdd.collect() )
        if len(pdt_IDs) > 0:
            pdt_query = '(SELECT * FROM product_dim WHERE product_id IN ({}) AND active_flag) as query'.format(pdt_IDs)
            pdt_temp_df = spark.read.jdbc(url=url, table=pdt_query, properties=mysql_properties)
            
            if pdt_temp_df.isEmpty():
                logging.warning(f"Product information isn't present. Please load product data in database")
                raise HTTPException(status_code=400, detail=f"Product information isn't present. Please load product data in database")

            pdt_int_df = integrated_workforce_performance_df.join(pdt_temp_df, integrated_workforce_performance_df.product_id == pdt_temp_df.product_id, "left")\
                                    .select(integrated_workforce_performance_df["*"], pdt_temp_df["product_key_id"].alias("new_ID"))
            
            integrated_workforce_performance_df = pdt_int_df.drop("product_id").withColumnRenamed("new_ID", "product_id")
        
        # Adding surrogate keys for channel
        ch_IDs = ','.join("'{}'".format(x[0]) for x in integrated_workforce_performance_df.select("channel_id").rdd.collect() )
        if len(ch_IDs) > 0:
            ch_query = '(SELECT * FROM channels_dim WHERE channel_id IN ({}) AND active_flag) as query'.format(ch_IDs)
            ch_temp_df = spark.read.jdbc(url=url, table=ch_query, properties=mysql_properties)
            
            if ch_temp_df.isEmpty():
                logging.warning(f"channel information isn't present. Please load product data in database")
                raise HTTPException(status_code=400, detail=f"Product information isn't present. Please load product data in database")

            ch_int_df = integrated_workforce_performance_df.join(ch_temp_df, integrated_workforce_performance_df.channel_id == ch_temp_df.channel_id, "left")\
                                    .select(integrated_workforce_performance_df["*"], ch_temp_df["channel_key_id"].alias("new_ID"))
            
            integrated_workforce_performance_df = ch_int_df.drop("channel_id").withColumnRenamed("new_ID", "channel_id")

        print(integrated_workforce_performance_df.show(2))
        print(integrated_workforce_performance_df.printSchema())

# adding surrogate keys to integrated_interaction_fact_df
        # ADDING SURROGATE KEY OF DIMENSION TABLES as foreign key to integrated df
        integration_ids =", ".join("'{}'".format(x[0]) for x in integrated_interaction_df.select("interaction_id").rdd.collect())
        if len(integration_ids) > 0:
            query = '(SELECT * FROM interaction_fact WHERE interaction_id IN ({})) as query'.format(integration_ids)
            df = spark.read.jdbc(url=url, table=query, properties=mysql_properties)
            ids = str([row.interaction_id for row in df.select("interaction_id").collect()])[1:-1]

            if df.isEmpty():
                print("New data is going to load in Database")
            else:
                logging.warning(f"The incoming data from source is already present in main integrated fact table for following  interaction_ids :{ids}")
                raise HTTPException(status_code=400,
                                    detail=f"The incoming data from source is already present in main integrated fact table for following  interaction_ids :{ids}")

        print(integrated_interaction_df.show(2))
        print(integrated_interaction_df.printSchema())

        # Adding surrogate keys for customer
        cust_IDs = ",".join("'{}'".format(x[0]) for x in integrated_interaction_df.select("customer_id").rdd.collect())
        if len(cust_IDs) > 0:
            cust_query = '(SELECT * FROM customer_crm_dim WHERE customer_id IN ({}) AND active_flag) as query'.format(cust_IDs)
            cust_temp_df = spark.read.jdbc(url=url, table=cust_query, properties=mysql_properties)
            
            if cust_temp_df.isEmpty():
                logging.warning(f"Customer information isn't present. Please load customer data in database")
                raise HTTPException(status_code=400, detail=f"Customer information isn't present. Please load customer data in database")
            cust_int_df = integrated_interaction_df.join(cust_temp_df, integrated_interaction_df.customer_id == cust_temp_df.customer_id, "left") \
                                    .select(integrated_interaction_df["*"], cust_temp_df["customer_key_id"].alias("new_CustomerID"))
            
            integrated_interaction_df = cust_int_df.drop("customer_id").withColumnRenamed("new_CustomerID", "customer_id")

        # Adding surrogate keys for agent
        user_ids = ",".join("'{}'".format(x[0]) for x in integrated_interaction_df.select("user_id").rdd.collect())
        if len(user_ids) > 0:
            agent_query = '(SELECT * FROM user_dim WHERE user_id IN ({}) AND active_flag) as query'.format(user_ids)
            agent_temp_df = spark.read.jdbc(url=url, table=agent_query, properties=mysql_properties)
            
            if agent_temp_df.isEmpty():
                logging.warning(f"Agent information isn't present. Please load agent data in database")
                raise HTTPException(status_code=400, detail=f"Agent information isn't present. Please load agent data in database")

            agent_int_df = integrated_interaction_df.join(agent_temp_df, integrated_interaction_df.user_id == agent_temp_df.user_id,"left")\
                                        .select(integrated_interaction_df["*"], agent_temp_df["user_key_id"].alias("new_ID"))
            
            integrated_interaction_df = agent_int_df.drop("user_id").withColumnRenamed("new_ID", "user_id")

        # Adding surrogate keys for product
        pdt_IDs = ','.join("'{}'".format(x[0]) for x in integrated_interaction_df.select("product_id").rdd.collect() if x[0] is not None)
        if len(pdt_IDs) > 0:
            pdt_query = '(SELECT * FROM product_dim WHERE product_id IN ({}) AND active_flag) as query'.format(pdt_IDs)
            pdt_temp_df = spark.read.jdbc(url=url, table=pdt_query, properties=mysql_properties)
            
            if pdt_temp_df.isEmpty():
                logging.warning(f"Product information isn't present. Please load product data in database")
                raise HTTPException(status_code=400, detail=f"Product information isn't present. Please load product data in database")

            pdt_int_df = integrated_interaction_df.join(pdt_temp_df, integrated_interaction_df.product_id == pdt_temp_df.product_id, "left")\
                                    .select(integrated_interaction_df["*"], pdt_temp_df["product_key_id"].alias("new_ID"))
            
            integrated_interaction_df = pdt_int_df.drop("product_id").withColumnRenamed("new_ID", "product_id")


        # Adding surrogate keys for org
        org_IDs= ",".join("'{}'".format(x[0]) for x in integrated_interaction_df.select("organisation_id").rdd.collect())
        if len(org_IDs) > 0:
            org_query = '(SELECT * FROM organisation_dim WHERE organisation_id IN ({}) AND active_flag) as query'.format(org_IDs)
            org_temp_df = spark.read.jdbc(url=url, table=org_query, properties=mysql_properties)
            
            if org_temp_df.isEmpty():
                logging.warning(f"organisation information isn't present. Please load organisation data in database")
                raise HTTPException(status_code=400, detail=f"organisation information isn't present. Please load organisation data in database")

            org_int_df = integrated_interaction_df.join(org_temp_df, integrated_interaction_df.organisation_id == org_temp_df.organisation_id, "left")\
                                    .select(integrated_interaction_df["*"], org_temp_df["org_key_id"].alias("new_ID"))
            
            integrated_interaction_df = org_int_df.drop("organisation_id").withColumnRenamed("new_ID", "organisation_id")
        
        # Adding surrogate keys for client
        client_IDs = ",".join("'{}'".format(x[0]) for x in integrated_interaction_df.select("client_id").rdd.collect())
        if len(client_IDs) > 0:
            client_query = '(SELECT * FROM client_detail_dim WHERE client_id IN ({}) AND active_flag) as query'.format(user_ids)
            client_temp_df = spark.read.jdbc(url=url, table=client_query, properties=mysql_properties)
            
            if client_temp_df.isEmpty():
                logging.warning(f"client information isn't present. Please load agent data in database")
                raise HTTPException(status_code=400, detail=f"client information isn't present. Please load agent data in database")

            client_int_df = integrated_interaction_df.join(client_temp_df, integrated_interaction_df.client_id == client_temp_df.client_id,"left")\
                                        .select(integrated_interaction_df["*"], client_temp_df["client_key_id"].alias("new_ID"))
            
            integrated_interaction_df = client_int_df.drop("client_id").withColumnRenamed("new_ID", "client_id")

        # Adding surrogate keys for channel
        ch_IDs = ','.join("'{}'".format(x[0]) for x in integrated_workforce_performance_df.select("channel_id").rdd.collect() if x[0] is not None)
        if len(ch_IDs) > 0:
            ch_query = '(SELECT * FROM channels_dim WHERE channel_id IN ({}) AND active_flag) as query'.format(ch_IDs)
            ch_temp_df = spark.read.jdbc(url=url, table=ch_query, properties=mysql_properties)
            
            if ch_temp_df.isEmpty():
                logging.warning(f"channel information isn't present. Please load channel data in database")
                raise HTTPException(status_code=400, detail=f"channel information isn't present. Please load channel data in database")

            ch_int_df = integrated_interaction_df.join(ch_temp_df, integrated_interaction_df.channel_id == ch_temp_df.channel_id, "left")\
                                    .select(integrated_interaction_df["*"], ch_temp_df["channel_key_id"].alias("new_ID"))
            
            integrated_interaction_df = ch_int_df.drop("channel_id").withColumnRenamed("new_ID", "channel_id")

        print(integrated_interaction_df.show(2))
        print(integrated_interaction_df.printSchema())

# adding surrogate keys to integrated_cust_experience_fact_df
        # ADDING SURROGATE KEY OF DIMENSION TABLES as foreign key to integrated df
        integration_ids =", ".join("'{}'".format(x[0]) for x in integrated_cust_experience_df.select("interaction_id").rdd.collect())
        if len(integration_ids) > 0:
            query = '(SELECT * FROM cust_experience_fact WHERE interaction_id IN ({})) as query'.format(integration_ids)
            df = spark.read.jdbc(url=url, table=query, properties=mysql_properties)
            ids = str([row.interaction_id for row in df.select("interaction_id").collect()])[1:-1]

            if df.isEmpty():
                print("New data is going to load in Database")
            else:
                logging.warning(f"The incoming data from source is already present in main integrated fact table for following  interaction_ids :{ids}")
                raise HTTPException(status_code=400,
                                    detail=f"The incoming data from source is already present in main integrated fact table for following  interaction_ids :{ids}")

        # print(integrated_df.show(2))
        # print(integrated_df.printSchema())

        # Adding surrogate keys for customer
        cust_IDs = ",".join("'{}'".format(x[0]) for x in integrated_cust_experience_df.select("customer_id").rdd.collect())
        if len(cust_IDs) > 0:
            cust_query = '(SELECT * FROM customer_crm_dim WHERE customer_id IN ({}) AND active_flag) as query'.format(cust_IDs)
            cust_temp_df = spark.read.jdbc(url=url, table=cust_query, properties=mysql_properties)
            
            if cust_temp_df.isEmpty():
                logging.warning(f"Customer information isn't present. Please load customer data in database")
                raise HTTPException(status_code=400, detail=f"Customer information isn't present. Please load customer data in database")
            cust_int_df = integrated_cust_experience_df.join(cust_temp_df, integrated_cust_experience_df.customer_id == cust_temp_df.customer_id, "left") \
                                    .select(integrated_cust_experience_df["*"], cust_temp_df["customer_key_id"].alias("new_CustomerID"))
            
            integrated_cust_experience_df = cust_int_df.drop("customer_id").withColumnRenamed("new_CustomerID", "customer_id")



        # Adding surrogate keys for product
        pdt_IDs = ','.join("'{}'".format(x[0]) for x in integrated_cust_experience_df.select("product_id").rdd.collect() if x[0] is not None)
        if len(pdt_IDs) > 0:
            pdt_query = '(SELECT * FROM product_dim WHERE product_id IN ({}) AND active_flag) as query'.format(pdt_IDs)
            pdt_temp_df = spark.read.jdbc(url=url, table=pdt_query, properties=mysql_properties)
            
            if pdt_temp_df.isEmpty():
                logging.warning(f"Product information isn't present. Please load product data in database")
                raise HTTPException(status_code=400, detail=f"Product information isn't present. Please load product data in database")

            pdt_int_df = integrated_cust_experience_df.join(pdt_temp_df, integrated_cust_experience_df.product_id == pdt_temp_df.product_id, "left")\
                                    .select(integrated_cust_experience_df["*"], pdt_temp_df["product_key_id"].alias("new_ID"))
            
            integrated_cust_experience_df = pdt_int_df.drop("product_id").withColumnRenamed("new_ID", "product_id")

        # Adding surrogate keys for org
        org_IDs= ",".join("'{}'".format(x[0]) for x in integrated_cust_experience_df.select("organisation_id").rdd.collect())
        if len(org_IDs) > 0:
            org_query = '(SELECT * FROM organisation_dim WHERE organisation_id IN ({}) AND active_flag) as query'.format(org_IDs)
            org_temp_df = spark.read.jdbc(url=url, table=org_query, properties=mysql_properties)
            
            if org_temp_df.isEmpty():
                logging.warning(f"organisation information isn't present. Please load organisation data in database")
                raise HTTPException(status_code=400, detail=f"organisation information isn't present. Please load organisation data in database")

            org_int_df = integrated_cust_experience_df.join(org_temp_df, integrated_cust_experience_df.organisation_id == org_temp_df.organisation_id, "left")\
                                    .select(integrated_cust_experience_df["*"], org_temp_df["org_key_id"].alias("new_ID"))
            
            integrated_cust_experience_df = org_int_df.drop("organisation_id").withColumnRenamed("new_ID", "organisation_id")

        # Adding surrogate keys for channel
        ch_IDs = ','.join("'{}'".format(x[0]) for x in integrated_cust_experience_df.select("channel_id").rdd.collect() if x[0] is not None)
        if len(ch_IDs) > 0:
            ch_query = '(SELECT * FROM channels_dim WHERE channel_id IN ({}) AND active_flag) as query'.format(ch_IDs)
            ch_temp_df = spark.read.jdbc(url=url, table=ch_query, properties=mysql_properties)
            
            if ch_temp_df.isEmpty():
                logging.warning(f"channel information isn't present. Please load channel data in database")
                raise HTTPException(status_code=400, detail=f"channel information isn't present. Please load channel data in database")

            ch_int_df = integrated_cust_experience_df.join(ch_temp_df, integrated_cust_experience_df.channel_id == ch_temp_df.channel_id, "left")\
                                    .select(integrated_cust_experience_df["*"], ch_temp_df["channel_key_id"].alias("new_ID"))
            
            integrated_cust_experience_df = ch_int_df.drop("channel_id").withColumnRenamed("new_ID", "channel_id")

        print(integrated_cust_experience_df.show(2))
        print(integrated_cust_experience_df.printSchema())

        logging.info("Call channel data successfully integrated and transformed and ready to load in database")
        return integrated_workforce_performance_df, integrated_interaction_df, integrated_cust_experience_df

