import mysql.connector
import utils

#User -->
class user_scd:

    def __init__(self):
        # Spark session is initialized from utils
        self.spark = utils.create_session()

    def scd_transform(self, agent_source, table_name):
        # Call the upsert function with the correct table name
        print("i am in scd")
        self.upsert_to_mysql(agent_source, table_name)

    @staticmethod
    def update_records(partition, table_name):
        """
        Function to run update queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where updates need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Update query to deactivate old records by setting active_flag = 0
        update_query = f"""
        UPDATE {table_name}
        SET active_flag = 0, end_date = CURDATE()
        WHERE user_id = %s AND active_flag = 1;
        """

        # Execute update queries for each row in the partition
        # A partition here is a list of rows (in the form of tuples)
        for row in partition:                   
            cursor.execute(update_query, (row.user_id,))

        #(row.agent_id,): This is the tuple of values being passed to the query, which in this case is the agent_id 
        # from the row. This agent_id is used to match records in the MySQL table that need to be updated.

        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()

    @staticmethod
    def insert_records(partition, table_name):
        """
        Function to run insert queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where inserts need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # # Insert query for new agent records
        # insert_query = f"""
        # INSERT INTO {table_name} (user_id, role_name, first_name,last_name, email, 
        #                             is_active,date_joined, user_email, organisation_id,
        #                             customer_id,user_processes, empcode ,user_mobile,tenure ,bucket  
        #                             status, active_flag, start_date, end_date)
        # VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s, 1, CURDATE(), '9999-12-31');
        # """

        insert_query = f"""
        INSERT INTO {table_name} (user_id, role_id, role_name, first_name, last_name, email, 
                                    is_active, date_joined, user_email, organisation_id,  
                                    customer_id, user_processes, empcode, user_mobile, tenure, bucket,  
                                    status, active_flag, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, CURDATE(), '9999-12-31');
        """


        # Execute insert queries for each row in the partition
        # for row in partition:
        #     cursor.execute(insert_query,(
        #         row.user_id, row.role_name, row.first_name, row.last_name, row.email, row.is_active, 
        #         row.date_joined, row.user_email, row.organisation_id, row.customer_id, 
        #         row.user_processes, row.empcode,row.user_mobile,row.tenure,row.bucket,
        #         row.status
        #     ))
        for row in partition:
            cursor.execute(insert_query, (
                row.user_id, row.role_id, row.role_name, row.first_name, row.last_name, row.email, row.is_active, 
                row.date_joined, row.user_email, row.organisation_id, row.customer_id, 
                row.user_processes, row.empcode, row.user_mobile, row.tenure, row.bucket,
                row.status if row.status is not None else None  # This replaces None values with SQL NULL
            ))

        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()

    def upsert_to_mysql(self, df, table_name):
        """
        Upsert function to update and insert records from Spark DataFrame into MySQL table.
        :param df: Spark DataFrame containing new data.
        :param table_name: Name of the MySQL table where upsert happens.
        """
        # Step 1: Load existing data from MySQL using JDBC from utils
        existing_data_df = self.spark.read \
            .format("jdbc") \
            .option("url", utils.url) \
            .option("dbtable", table_name) \
            .option("user", utils.user) \
            .option("password", utils.password) \
            .option("driver", utils.mysql_properties['driver']) \
            .load()
        
        
        # Step 2: Identify rows for update (existing agent_id)
        update_df = df.join(existing_data_df, on='user_id', how='inner')
        # print("---------------update_df in scd -------------------")
        # update_df.show(50,truncate=False)
        
        
        # Step 3: Identify rows for insert (new agent_id)
        #insert_df = df.join(existing_data_df, on='agent_id', how='left_anti')
        # print("---------------insert_df in scd df -------------------")
        # insert_df.show(50,truncate=False)
        

        # Step 4: Run update and insert actions
        # Spark processes these partitions in parallel, making the operation faster, especially on large datasets.
        # Why Use foreachPartition?

        # 1. Using foreachPartition is more efficient in distributed computing frameworks like Spark. 
        # 2. It allows you to open the database connection once per partition (instead of for every single row), 
        # 3. It also allows partitions to be  processed in parallel, which improves performance for large datasets

        update_df.foreachPartition(lambda partition: user_scd.update_records(partition, table_name))
        df.foreachPartition(lambda partition: user_scd.insert_records(partition, table_name))

        print(f"Upsert operation completed for table: {table_name}")


#Client -->
class client_scd:

    def __init__(self):
        # Spark session is initialized from utils
        self.spark = utils.create_session()

    def scd_transform_client(self, client_source, table_name_client):
        # Call the upsert function with the correct table name
        self.upsert_to_mysql(client_source, table_name_client)

    @staticmethod
    def update_records(partition, table_name_client):
        """
        Function to run update queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where updates need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Update query to deactivate old records by setting active_flag = 0
        update_query = f"""
        UPDATE {table_name_client}
        SET active_flag = 0, end_date = CURDATE()
        WHERE client_id = %s AND active_flag = 1;
        """

        # Execute update queries for each row in the partition
        # A partition here is a list of rows (in the form of tuples)
        for row in partition:                   
            cursor.execute(update_query, (row.client_id,))

        #(row.agent_id,): This is the tuple of values being passed to the query, which in this case is the agent_id 
        # from the row. This agent_id is used to match records in the MySQL table that need to be updated.

        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()

    @staticmethod
    def insert_records(partition, table_name_client):
        """
        Function to run insert queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where inserts need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # # Insert query for new agent records
        # insert_query = f"""
        # INSERT INTO {table_name} (user_id, role_name, first_name,last_name, email, 
        #                             is_active,date_joined, user_email, organisation_id,
        #                             customer_id,user_processes, empcode ,user_mobile,tenure ,bucket  
        #                             status, active_flag, start_date, end_date)
        # VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s, 1, CURDATE(), '9999-12-31');
        # """

        insert_query = f"""
        INSERT INTO {table_name_client} (client_id, organisation_id, client_first_name, client_last_name, 
                                            client_language, 
                                            client_email, client_mobile, 
                                            active_flag, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 1, CURDATE(), '9999-12-31');
        """


        for row in partition:
            cursor.execute(insert_query, (
                row.client_id, row.organisation_id, row.client_first_name, row.client_last_name, 
                row.client_language, row.client_email, 
                row.client_mobile
            ))

        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()

    def upsert_to_mysql(self, df, table_name_client):
        """
        Upsert function to update and insert records from Spark DataFrame into MySQL table.
        :param df: Spark DataFrame containing new data.
        :param table_name: Name of the MySQL table where upsert happens.
        """
        # Step 1: Load existing data from MySQL using JDBC from utils
        existing_data_df = self.spark.read \
            .format("jdbc") \
            .option("url", utils.url) \
            .option("dbtable", table_name_client) \
            .option("user", utils.user) \
            .option("password", utils.password) \
            .option("driver", utils.mysql_properties['driver']) \
            .load()
        
        
        # Step 2: Identify rows for update (existing agent_id)
        update_df = df.join(existing_data_df, on='client_id', how='inner')
        # print("---------------update_df in scd -------------------")
        # update_df.show(50,truncate=False)
        
        
        # Step 3: Identify rows for insert (new agent_id)
        #insert_df = df.join(existing_data_df, on='agent_id', how='left_anti')
        # print("---------------insert_df in scd df -------------------")
        # insert_df.show(50,truncate=False)
        

        # Step 4: Run update and insert actions
        # Spark processes these partitions in parallel, making the operation faster, especially on large datasets.
        # Why Use foreachPartition?

        # 1. Using foreachPartition is more efficient in distributed computing frameworks like Spark. 
        # 2. It allows you to open the database connection once per partition (instead of for every single row), 
        # 3. It also allows partitions to be  processed in parallel, which improves performance for large datasets

        update_df.foreachPartition(lambda partition: client_scd.update_records(partition, table_name_client))
        df.foreachPartition(lambda partition: client_scd.insert_records(partition, table_name_client))

        print(f"Upsert operation completed for table: {table_name_client}")



# org -->
class org_scd:

    def __init__(self):
        # Spark session is initialized from utils
        self.spark = utils.create_session()

    def scd_transform_org(self, org_source, table_name_org):
        # Call the upsert function with the correct table name
        self.upsert_to_mysql_org(org_source, table_name_org)

    @staticmethod
    def update_records_org(partition, table_name_org):
        """
        Function to run update queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where updates need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Update query to deactivate old records by setting active_flag = 0
        update_query_org = f"""
        UPDATE {table_name_org}
        SET active_flag = 0, end_date = CURDATE()
        WHERE organisation_id = %s AND active_flag = 1;
        """

        # Execute update queries for each row in the partition
        for row in partition:
            cursor.execute(update_query_org, (row.organisation_id,))

        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()

    @staticmethod
    def insert_records_org(partition, table_name_org):
        """
        Function to run insert queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where inserts need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Insert query for new agent records
        insert_query_org = f"""
        INSERT INTO {table_name_org} (
            organisation_id, 
            organisation_name,  
            organisation_email, 
            organisation_mobile, 
            organisation_address,  
            organisation_city,
            organisation_state,  
            organisation_country,  
            organisation_pincode,
            active_flag, 
            start_date, 
            end_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, CURDATE(), '9999-12-31');
        """

        for row in partition:
            cursor.execute(insert_query_org, (
                row.organisation_id, row.organisation_name, row.organisation_email, 
                row.organisation_mobile, 
                row.organisation_address, 
                row.organisation_city, row.organisation_state, row.organisation_country, row.organisation_pincode
                ))

        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()

    def upsert_to_mysql_org(self, df_org, table_name_org):
        """
        Upsert function to update and insert records from Spark DataFrame into MySQL table.
        :param df: Spark DataFrame containing new data.
        :param table_name: Name of the MySQL table where upsert happens.
        """
        # Step 1: Load existing data from MySQL using JDBC from utils
        existing_data_df_org = self.spark.read \
            .format("jdbc") \
            .option("url", utils.url) \
            .option("dbtable", table_name_org) \
            .option("user", utils.user) \
            .option("password", utils.password) \
            .option("driver", utils.mysql_properties['driver']) \
            .load()
        
        
        # Step 2: Identify rows for update (existing agent_id)
        update_df_org = df_org.join(existing_data_df_org, on='organisation_id', how='inner')
        # print("--------------- org update_df in scd -------------------")
        # update_df_org.show(50,truncate=False)
        
        
        # Step 3: Identify rows for insert (new agent_id)
        #insert_df_org = df_org.join(existing_data_df_org, on='OrganizationID', how='left_anti')
        # print("--------------- org insert_df in scd df -------------------")
        # insert_df_org.show(50,truncate=False)
        

        # Step 4: Run update and insert actions
        update_df_org.foreachPartition(lambda partition: org_scd.update_records_org(partition, table_name_org))
        df_org.foreachPartition(lambda partition: org_scd.insert_records_org(partition, table_name_org))

        print(f"Upsert operation completed for table: {table_name_org}")




# prod -->
class product_scd:

    def __init__(self):
        # Spark session is initialized from utils
        self.spark = utils.create_session()

    def scd_transform_prod(self, prod_source, table_name_prod):
        # Call the upsert function with the correct table name
        self.upsert_to_mysql_prod(prod_source, table_name_prod)

    @staticmethod
    def update_records_prod(partition, table_name_prod):
        """
        Function to run update queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where updates need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Update query to deactivate old records by setting active_flag = 0
        update_query_org = f"""
        UPDATE {table_name_prod}
        SET active_flag = 0, end_date = CURDATE()
        WHERE product_id = %s AND active_flag = 1;
        """

        # Execute update queries for each row in the partition
        for row in partition:
            cursor.execute(update_query_org, (row.product_id,))

        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()

    @staticmethod
    def insert_records_prod(partition, table_name_prod):
        """
        Function to run insert queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where inserts need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Insert query for new agent records
        insert_query_org = f"""
        INSERT INTO {table_name_prod} (
            product_id,
            organisation_id,
            product_name,
            category,
            description,
            owner,
            price,
            active_flag, 
            start_date, 
            end_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 1, CURDATE(), '9999-12-31');
        """

        for row in partition:
            cursor.execute(insert_query_org, (
                row.product_id, row.organisation_id, row.product_name, 
                row.category, 
                row.description, 
                row.owner, row.price
                ))

        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()

    def upsert_to_mysql_prod(self, df_prod, table_name_prod):
        """
        Upsert function to update and insert records from Spark DataFrame into MySQL table.
        :param df: Spark DataFrame containing new data.
        :param table_name: Name of the MySQL table where upsert happens.
        """
        # Step 1: Load existing data from MySQL using JDBC from utils
        existing_data_df_org = self.spark.read \
            .format("jdbc") \
            .option("url", utils.url) \
            .option("dbtable", table_name_prod) \
            .option("user", utils.user) \
            .option("password", utils.password) \
            .option("driver", utils.mysql_properties['driver']) \
            .load()
        
        
        # Step 2: Identify rows for update (existing agent_id)
        update_df_prod = df_prod.join(existing_data_df_org, on='product_id', how='inner')
        # print("--------------- org update_df in scd -------------------")
        # update_df_org.show(50,truncate=False)
        
        
        # Step 3: Identify rows for insert (new agent_id)
        #insert_df_org = df_org.join(existing_data_df_org, on='OrganizationID', how='left_anti')
        # print("--------------- org insert_df in scd df -------------------")
        # insert_df_org.show(50,truncate=False)
        

        # Step 4: Run update and insert actions
        update_df_prod.foreachPartition(lambda partition: product_scd.update_records_prod(partition, table_name_prod))
        df_prod.foreachPartition(lambda partition: product_scd.insert_records_prod(partition, table_name_prod))

        print(f"Upsert operation completed for table: {table_name_prod}")




# crm -->
class crm_scd:

    def __init__(self):
        # Spark session is initialized from utils
        self.spark = utils.create_session()

    def scd_transform_crm(self, crm_source, table_name_crm):
        # Call the upsert function with the correct table name
        self.upsert_to_mysql_cust(crm_source, table_name_crm)

    @staticmethod
    def update_records_cust(partition, table_name_crm):
        """
        Function to run update queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where updates need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Update query to deactivate old records by setting active_flag = 0
        update_query_cust = f"""
        UPDATE {table_name_crm}
        SET active_flag = 0, end_date = CURDATE()
        WHERE customer_id = %s AND active_flag = 1;
        """

        # Execute update queries for each row in the partition
        for row in partition:
            cursor.execute(update_query_cust, (row.customer_id,))

        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()

    @staticmethod
    def insert_records_cust(partition, table_name_crm):
        """
        Function to run insert queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where inserts need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Insert query for new agent records
        insert_query_cust = f"""
        INSERT INTO {table_name_crm} (
        customer_id, first_name, last_name, email, phone_number, DOB, gender, address, 
        city, state, country, postal_code, customer_income, preferred_language, 
        total_spent,social_profile,customer_since, customer_status,customer_churn, active_flag, start_date, end_date
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s ,%s ,%s , 1, CURDATE(), '9999-12-31');
        """
        
        for row in partition:
            cursor.execute(insert_query_cust, (
                row.customer_id, row.first_name, row.last_name, row.email, row.phone_number, row.DOB, row.gender, 
                row.address, row.city, row.state, row.country, row.postal_code, 
                row.customer_income, row.preferred_language, row.total_spent, row.social_profile,
                row.customer_since, row.customer_status, row.customer_churn
                ))


        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()

    def upsert_to_mysql_cust(self, df_cust, table_name_crm):
        """
        Upsert function to update and insert records from Spark DataFrame into MySQL table.
        :param df: Spark DataFrame containing new data.
        :param table_name: Name of the MySQL table where upsert happens.
        """
        # Step 1: Load existing data from MySQL using JDBC from utils
        existing_data_df_cust = self.spark.read \
            .format("jdbc") \
            .option("url", utils.url) \
            .option("dbtable", table_name_crm) \
            .option("user", utils.user) \
            .option("password", utils.password) \
            .option("driver", utils.mysql_properties['driver']) \
            .load()
        
        
        # Step 2: Identify rows for update (existing agent_id)
        update_df = df_cust.join(existing_data_df_cust, on='customer_id', how='inner')
        # print("---------------Customer update_df in scd -------------------")
        # update_df.show(50,truncate=False)
        
        
        # Step 3: Identify rows for insert (new agent_id)
        #insert_df = df_cust.join(existing_data_df_cust, on='CustomerID', how='left_anti')
        # print("---------------Customer insert_df in scd df -------------------")
        # insert_df.show(50,truncate=False)
        

        # Step 4: Run update and insert actions
        update_df.foreachPartition(lambda partition: crm_scd.update_records_cust(partition, table_name_crm))
        df_cust.foreachPartition(lambda partition: crm_scd.insert_records_cust(partition, table_name_crm))

        print(f"Upsert operation completed for table: {table_name_crm}")


# channel --->
class channel_scd:
 
    def __init__(self):
        # Spark session is initialized from utils
        self.spark = utils.create_session()
 
    def scd_transform_channel(self, channel_source, table_name_channel):
        # Call the upsert function with the correct table name
        self.upsert_to_mysql_channel(channel_source, table_name_channel)
 
    @staticmethod
    def update_records_channel(partition, table_name_channel):
        """
        Function to run update queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where updates need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
 
        # Update query to deactivate old records by setting active_flag = 0
        update_query_channel = f"""
        UPDATE {table_name_channel}
        SET active_flag = 0, end_date = CURDATE()
        WHERE channel_id = %s AND active_flag = 1;
        """
 
        # Execute update queries for each row in the partition
        for row in partition:
            cursor.execute(update_query_channel, (row.channel_id,))
 
        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()
 
    @staticmethod
    def insert_records_channel(partition, table_name_channel):
        """
        Function to run insert queries on the MySQL database.
        :param partition: A list of rows (tuples) for a partition.
        :param table_name: Name of the table where inserts need to happen.
        """
        # Establish connection to MySQL using credentials from utils
        db_config = {
            "host": utils.host,
            "user": utils.user,
            "password": utils.password,
            "database": utils.database
        }
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
 
        # Insert query for new agent records
        insert_query_channel = f"""
        INSERT INTO {table_name_channel} (
        channel_id, channel_name, active_flag, start_date, end_date
        )
        VALUES (%s ,%s , 1, CURDATE(), '9999-12-31');
        """
       
        for row in partition:
            cursor.execute(insert_query_channel, (
                        row.channel_id, row.channel_name
                        ))
 
 
        conn.commit()  # Commit the transaction
        cursor.close()
        conn.close()
 
    def upsert_to_mysql_channel(self, df_channel, table_name_channel):
        """
        Upsert function to update and insert records from Spark DataFrame into MySQL table.
        :param df: Spark DataFrame containing new data.
        :param table_name: Name of the MySQL table where upsert happens.
        """
        # Step 1: Load existing data from MySQL using JDBC from utils
        existing_data_df_cust = self.spark.read \
            .format("jdbc") \
            .option("url", utils.url) \
            .option("dbtable", table_name_channel) \
            .option("user", utils.user) \
            .option("password", utils.password) \
            .option("driver", utils.mysql_properties['driver']) \
            .load()
       
       
        # Step 2: Identify rows for update
        update_df = df_channel.join(existing_data_df_cust, on='channel_id', how='inner')
        # print("---------------Customer update_df in scd -------------------")
        # update_df.show(50,truncate=False)
       
       
        # Step 3: Identify rows for insert (new agent_id)
        #insert_df = df_cust.join(existing_data_df_cust, on='CustomerID', how='left_anti')
        # print("---------------Customer insert_df in scd df -------------------")
        # insert_df.show(50,truncate=False)
       
 
        # Step 4: Run update and insert actions
        update_df.foreachPartition(lambda partition: channel_scd.update_records_channel(partition, table_name_channel))
        df_channel.foreachPartition(lambda partition: channel_scd.insert_records_channel(partition, table_name_channel))
 
        print(f"Upsert operation completed for table: {table_name_channel}")