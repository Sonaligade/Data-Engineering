import mysql.connector
import utils

# database connection
url = utils.url
host = utils.host
user = utils.user
password = utils.password
database = utils.database

db_config = {
    "host": host,
    "user":user,
    "password": password,
    "database": database}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# ********************************* CREATING LOGS TABLE ************************************************

create_logs_table = """
CREATE TABLE IF NOT EXISTS logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    log_time DATETIME NOT NULL,
    line_number INT NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    log_level VARCHAR(50) NOT NULL,
    log_message TEXT NOT NULL
);
"""

cursor.execute(create_logs_table)
conn.commit()
print("logs Table created successfully")

#  ************************************************ audit log ************************************************

create_audit_logs_table = """
CREATE TABLE IF NOT EXISTS audit_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user VARCHAR(50),
    audit_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    stage VARCHAR(255),
    action VARCHAR(255),
    description TEXT,
    status VARCHAR(50),
    duration FLOAT,
    app_id VARCHAR(100)
);
"""

# Execute the query
cursor.execute(create_audit_logs_table)
conn.commit()
print("Audit Logs Table created successfully")


#  ************************************************ creating dimension table  ************************************************


# user_dim table
create_user_dim = """
CREATE TABLE IF NOT EXISTS user_dim (
    user_key_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    role_id INT,
    role_name VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    is_active BOOLEAN,
    date_joined DATETIME,
    user_email VARCHAR(100),
    organisation_id INT,
    customer_id INT,
    user_processes VARCHAR(255),
    empcode TEXT,
    user_mobile VARCHAR(20),
    tenure VARCHAR(50),
    bucket VARCHAR(50),
    status VARCHAR(20),
    active_flag INT,
    start_date DATE,
    end_date DATE
);
"""
cursor.execute(create_user_dim)
conn.commit()
print("user_dim table created successfully")

# organization_dim table
create_organisation_dim = """
CREATE TABLE IF NOT EXISTS organisation_dim (
    org_key_id INT AUTO_INCREMENT PRIMARY KEY,
    organisation_id VARCHAR(255),
    organisation_name VARCHAR(255),
    organisation_email VARCHAR(255),
    organisation_mobile VARCHAR(255),
    organisation_address TEXT,
    organisation_city VARCHAR(255),
    organisation_state VARCHAR(255),
    organisation_country VARCHAR(255),
    organisation_pincode VARCHAR(255),
    active_flag INT,
    start_date DATE,
    end_date DATE
);
"""
cursor.execute(create_organisation_dim)
conn.commit()
print("organisation_dim table created successfully")

# product_dim table
create_product_dim = """
CREATE TABLE IF NOT EXISTS product_dim (
    product_key_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    organisation_id INT,
    product_name VARCHAR(255),
    category VARCHAR(255),
    description TEXT,
    owner VARCHAR(255),
    price VARCHAR(50),
    active_flag INT,
    start_date DATE,
    end_date DATE
);
"""
cursor.execute(create_product_dim)
conn.commit()
print("product_dim table created successfully")

# customer_crm_dim table
create_customer_crm_dim = """
CREATE TABLE IF NOT EXISTS customer_crm_dim (
    customer_key_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone_number VARCHAR(255) NOT NULL,
    DOB DATE,
    gender VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    postal_code VARCHAR(255),
    customer_income DECIMAL(20, 2),
    preferred_language VARCHAR(255),
    total_spent DECIMAL(20, 2),
    social_profile VARCHAR(255),
    customer_since TIMESTAMP,
    customer_status VARCHAR(255),
    customer_churn VARCHAR(255),
    active_flag INT,
    start_date DATE,
    end_date DATE
);
"""
cursor.execute(create_customer_crm_dim)
conn.commit()
print("customer_crm_dim table created successfully")

# client_detail table
create_client_detail_dim = """
CREATE TABLE IF NOT EXISTS client_detail_dim (
    client_key_id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT,
    organisation_id INT,
    client_first_name VARCHAR(255),
    client_last_name VARCHAR(255),
    client_language VARCHAR(255),
    client_email VARCHAR(255),
    client_mobile VARCHAR(255),
    active_flag INT,
    start_date DATE,
    end_date DATE
);
"""
cursor.execute(create_client_detail_dim)
conn.commit()
print("client_detail table created successfully")

# channels table
create_channels_dim = """
CREATE TABLE IF NOT EXISTS channels_dim (
    channel_key_id INT AUTO_INCREMENT PRIMARY KEY,
    channel_id INT,
    channel_name VARCHAR(255),
    active_flag INT,
    start_date DATE,
    end_date DATE
);
"""
cursor.execute(create_channels_dim)
conn.commit()
print("channels table created successfully")

# workforce_performance_fact table
create_workforce_performance_fact = """
CREATE TABLE IF NOT EXISTS workforce_performance_fact (
    interaction_id INT,
    user_id INT,
    organisation_id INT,
    product_id INT,
    channel_id INT,
    processes VARCHAR(255),
    agent_final_tone VARCHAR(255),
    agent_keyword VARCHAR(255),
    agent_sentiment VARCHAR(255),
    agent_sentiment_score BIGINT,
    average_compliance_score FLOAT,
    ai_feedback VARCHAR(255),
    ai_feedback_reason TEXT,
    manual_feedback TEXT,
    manual_rating BIGINT,
    agent_transcript TEXT,
    FOREIGN KEY (user_id) REFERENCES user_dim(user_key_id),
    FOREIGN KEY (organisation_id) REFERENCES organisation_dim(org_key_id),
    FOREIGN KEY (product_id) REFERENCES product_dim(product_key_id),
    FOREIGN KEY (channel_id) REFERENCES channels_dim(channel_key_id)
);
"""
cursor.execute(create_workforce_performance_fact)
conn.commit()
print("workforce_performance_fact table created successfully")

# interaction_fact table
create_interaction_fact = """
CREATE TABLE IF NOT EXISTS interaction_fact (
    interaction_id INT,
    interaction_file_id VARCHAR(255),
    customer_id INT,
    user_id INT,
    product_id INT,
    organisation_id INT,
    channel_id INT,
    client_id INT,
    department VARCHAR(255),
    wait_time VARCHAR(255),
    hold_time VARCHAR(255),
    dead_air TEXT,
    interaction_duration VARCHAR(20),
    interaction_type VARCHAR(255),
    interaction_date TIMESTAMP,
    interaction_channel VARCHAR(255),
    interaction_status VARCHAR(255),
    interaction_transferred VARCHAR(25),
    customer_intent VARCHAR(255),
    self_service VARCHAR(255),
    full_transcript TEXT,
    escalation VARCHAR(255),
    escalation_reason VARCHAR(255),
    language_detection VARCHAR(255),
    FOREIGN KEY (customer_id) REFERENCES customer_crm_dim(customer_key_id),
    FOREIGN KEY (user_id) REFERENCES user_dim(user_key_id),
    FOREIGN KEY (organisation_id) REFERENCES organisation_dim(org_key_id),
    FOREIGN KEY (product_id) REFERENCES product_dim(product_key_id),
    FOREIGN KEY (channel_id) REFERENCES channels_dim(channel_key_id),
    FOREIGN KEY (client_id) REFERENCES client_detail_dim(client_key_id)
);
"""
cursor.execute(create_interaction_fact)
conn.commit()
print("interaction_fact table created successfully")



# experience_fact table
create_cust_experience_fact = """
CREATE TABLE IF NOT EXISTS cust_experience_fact (
    interaction_id INT,
    customer_id INT,
    product_id INT,
    organisation_id INT,
    channel_id INT,
    csat_score BIGINT,
    ces_score BIGINT,
    nps_score BIGINT,
    customer_rating VARCHAR(25),
    customer_churn VARCHAR(255),
    customer_intent VARCHAR(255),
    customer_final_tone VARCHAR(255),
    customer_keywords VARCHAR(255),
    customer_sentiment VARCHAR(255),
    customer_sentiment_score BIGINT,
    customer_transcript TEXT,
    FOREIGN KEY (customer_id) REFERENCES customer_crm_dim(customer_key_id),
    FOREIGN KEY (organisation_id) REFERENCES organisation_dim(org_key_id),
    FOREIGN KEY (product_id) REFERENCES product_dim(product_key_id),
    FOREIGN KEY (channel_id) REFERENCES channels_dim(channel_key_id)
);
"""
cursor.execute(create_cust_experience_fact)
conn.commit()
print("experience_fact table created successfully")

# **********************************************   Corrupt Table  *********************************************************************

client_corrupt_detail = """
CREATE TABLE IF NOT EXISTS client_corrupt_detail (
    client_key_id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT,
    organisation_id INT,
    client_first_name VARCHAR(255),
    client_last_name VARCHAR(255),
    client_language VARCHAR(255),
    client_email VARCHAR(255),
    client_mobile VARCHAR(255),
    remark VARCHAR(1024)  -- Added this column to store the reason for corruption
);
"""
cursor.execute(client_corrupt_detail)
conn.commit()
print("client_corrupt_detail table created successfully")

org_corrupt_detail = """
CREATE TABLE IF NOT EXISTS org_corrupt_detail (
    org_key_id INT AUTO_INCREMENT PRIMARY KEY,
    organisation_id INT,
    organisation_name VARCHAR(255),
    organisation_email VARCHAR(255),
    organisation_mobile VARCHAR(255),
    organisation_address TEXT,
    organisation_city VARCHAR(255),
    organisation_state VARCHAR(255),
    organisation_country VARCHAR(255),
    organisation_pincode INT,
    remark VARCHAR(1024)  -- Added this column to store the reason for corruption
);
"""
cursor.execute(org_corrupt_detail)
conn.commit()
print("org_corrupt_detail table created successfully")

user_corrupt_detail = """
CREATE TABLE IF NOT EXISTS user_corrupt_detail (
    user_key_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    role_id INT,
    role_name VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    user_mobile VARCHAR(20),
    date_joined DATETIME,
    user_email VARCHAR(100),
    organisation_id INT,
    processes VARCHAR(255),
    empcode VARCHAR(20),
    tenure VARCHAR(50),
    bucket VARCHAR(50),
    status VARCHAR(20),
    remark VARCHAR(1024)  -- Added this column to store the reason for corruption
);
"""
cursor.execute(user_corrupt_detail)
conn.commit()
print("user_corrupt_detail table created successfully")

product_corrupt_detail = """
CREATE TABLE IF NOT EXISTS product_corrupt_detail (
    product_key_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    organisation_id INT,
    product_name VARCHAR(255),
    category VARCHAR(255),
    description TEXT,
    manufacturer VARCHAR(255),
    sku VARCHAR(255),
    price DECIMAL(10, 2),
    weight DECIMAL(10, 2),
    dimension VARCHAR(255),
    remark VARCHAR(1024)  -- Added this column to store the reason for corruption
);
"""
cursor.execute(product_corrupt_detail)
conn.commit()
print("product_corrupt_detail table created successfully")

customer_crm_corrupt_details = """
CREATE TABLE IF NOT EXISTS customer_crm_corrupt_details (
    customer_key_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(255),
    DOB DATE,
    gender VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255),
    postal_code VARCHAR(255),
    customer_income DECIMAL(10, 2), 
    preferred_language VARCHAR(255),
    total_spent DECIMAL(10, 2),
    social_profile VARCHAR(255),
    customer_since VARCHAR(255),
    customer_status VARCHAR(255),
    customer_churn VARCHAR(255),
    remark VARCHAR(1024)  -- Added this column to store the reason for corruption
);
"""
cursor.execute(customer_crm_corrupt_details)
conn.commit()
print("customer_crm_corrupt_details table created successfully")

omni_channel_corrupt_detail = """
CREATE TABLE IF NOT EXISTS omni_channel_corrupt_detail (
    id INT,
    interaction_file_id VARCHAR(255),
    agent_id INT,
    client_id INT,
    customer_id INT,
    organisation_id INT,
    self_service VARCHAR(255),
    product_id VARCHAR(255),
    channel_id INT,
    department VARCHAR(255),
    processes VARCHAR(255),
    agent_final_tone VARCHAR(255),
    cust_final_tone VARCHAR(255),
    interaction_duration VARCHAR(255),
    dead_air TEXT,
    average_compliance_score FLOAT,
    ai_feedback TEXT,
    ai_feedback_reason TEXT,
    customer_intent TEXT,
    escalation VARCHAR(255),
    escalation_reason TEXT,
    manual_feedback TEXT,
    manual_rating VARCHAR(255),
    customer_rating VARCHAR(255),
    interaction_date DATETIME,
    agent TEXT,
    customer TEXT,
    full TEXT,
    Sentiment TEXT,
    transcript_summary TEXT,
    keywords_tracking TEXT,
    language_detection VARCHAR(255),
    call_status VARCHAR(255),
    wait_time VARCHAR(255),
    hold_time VARCHAR(255),
    transfered_call VARCHAR(255),
    customer_satisfaction_rates FLOAT,
    customer_effort_score FLOAT,
    NPS_Score VARCHAR(255), 
    remark VARCHAR(1024)  -- Added this column to store the reason for corruption
);
"""
cursor.execute(omni_channel_corrupt_detail)
conn.commit()
print("omni_channel_corrupt_detail table created successfully")

cursor.close()
conn.close()