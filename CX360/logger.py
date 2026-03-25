import configparser
import logging
from datetime import datetime
import os, getpass
# import pymysql
import mysql.connector
from logging.handlers import TimedRotatingFileHandler

# ------------------------------- database connection --------------------------------------------------

config = configparser.ConfigParser() 
config.read("config.ini")

url = config.get("sql", "url")
host = config.get("sql", "host")
user = config.get("sql", "user")
password = config.get("sql", "password")
database = config.get("sql", "database")

# ------------------------------ Database logging handler --------------------------------------------------------------

class DBHandler(logging.Handler):
    def __init__(self):
        super().__init__()

    def emit(self, record):
        log_entry = self.format(record)
        self.save_log_to_db(record, log_entry)

    def save_log_to_db(self, record, log_entry):
        try:
            # Connect to MySQL database
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            cursor = connection.cursor()
            
            # Insert log into table
            query = "INSERT INTO logs (log_time, line_number, file_name, log_level, log_message) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(query, (
                datetime.now(), 
                record.lineno,  
                os.path.basename(record.pathname),  # Get the filename from the path
                record.levelname,       
                log_entry                
            ))
            connection.commit()

            cursor.close()
            connection.close()
        except Exception as e:
            file_logger.error("Error while saving log to DB: " + str(e), exc_info=True)

#------------------------------- Function to setup database logging -----------------------------------------------------

def setup_db_logging():
    db_logger = logging.getLogger('dblogger')

    if not any(isinstance(h, DBHandler) for h in db_logger.handlers):  # Prevent multiple handlers
        db_handler = DBHandler()
        db_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("[%(asctime)s] %(lineno)d - %(name)s - %(levelname)s - %(message)s")
        db_handler.setFormatter(formatter)
        db_logger.addHandler(db_handler)
    db_logger.setLevel(logging.INFO)

#------------------------------- Function to setup file logging -----------------------------------------------------

def setup_file_logger():
    # Set up a separate logger for file logging
    file_logger = logging.getLogger('fileLogger')
    
    # Ensure the logs directory exists
    logs_path = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(logs_path):
        os.makedirs(logs_path)
    
    log_file = os.path.join(logs_path, f"{datetime.now().date()}.log")

    # File handler with daily rotation
    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when='midnight',   # Rotate logs at midnight
        interval=1,        # Rotate every 1 day (create a new file each day)
        backupCount=0      # No backup, all files are kept
    )
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter("[%(asctime)s] %(lineno)d - %(filename)s - %(levelname)s - %(message)s")  # Include filename
    file_handler.setFormatter(file_formatter)

    # Add file and DB handlers if they are not already added
    if not any(isinstance(h, TimedRotatingFileHandler) for h in file_logger.handlers):
        file_logger.addHandler(file_handler)

    file_logger.setLevel(logging.INFO)

    return file_logger

#------------------------------- Set up both loggers --------------------------------------------

file_logger = setup_file_logger()
db_logger = setup_db_logging()

# ------------------------------ Audit logging handler --------------------------------------------------------------

class AuditHandler(logging.Handler):
    def __init__(self):
        super().__init__()

    def emit(self, record):
        audit_entry = record.getMessage()  # Only get the log message, not the extra fields
        self.save_audit_log_to_db(record, audit_entry)

    def save_audit_log_to_db(self, record, audit_entry):
        try:
            from utils import create_session
            app_id = create_session().sparkContext.applicationId
            local_user=getpass.getuser()

            # Connect to MySQL database
            connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database,
            )
            cursor = connection.cursor()

            # Insert audit log into audit_logs table, excluding extra fields from description
            query = """
            INSERT INTO audit_logs (user, audit_time, stage, action, description, status, duration, app_id)
            VALUES (%s,%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                local_user,
                datetime.now(),
                record.stage,  # Pipeline stage (MongoDB, Local, S3, etc.)
                record.action,  # Action taken (Read, Write, Transform)
                audit_entry,  # Detailed description (Only the message here)
                record.status,  # Status of the action (Success, Failed)
                record.duration,  # Duration of the action in seconds
                app_id
            ))
            connection.commit()

            cursor.close()
            connection.close()
        except Exception as e:
            print(f"Error while saving audit log to DB: {e}")


# ------------------------------- Function to setup audit logging -----------------------------------------------------

def setup_audit_logging():
    audit_logger = logging.getLogger('auditLogger')

    if not any(isinstance(h, AuditHandler) for h in audit_logger.handlers):  # Prevent multiple handlers
        audit_handler = AuditHandler()
        audit_handler.setLevel(logging.INFO)

        # Update Formatter to avoid including extra fields in the message part (description)
        audit_formatter = logging.Formatter(
            "[%(asctime)s] Stage: %(stage)s - Action: %(action)s - Status: %(status)s - Duration: %(duration).2f seconds")
        audit_handler.setFormatter(audit_formatter)

        audit_logger.addHandler(audit_handler)
    audit_logger.setLevel(logging.INFO)


# ------------------------------- Example of logging audit events with duration ----------------------------------

def log_audit_event(stage, action, status, description, duration):
    audit_logger = logging.getLogger('auditLogger')

    extra = {'stage': stage, 'action': action, 'status': status, 'duration': duration}

    # Log only the description in the message, and pass other fields via 'extra'
    audit_logger.info(description, extra=extra)