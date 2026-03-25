from fastapi import FastAPI, HTTPException
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, LongType, StructType, StructField, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col
import utils
import logging
import uvicorn

app = FastAPI()

@app.get("/load_crm_data/")
async def load_crm_data(file_path_crm: str):
    """Transform and serve CRM data for Power BI."""
    try:
        # Create Spark session
        spark = utils.create_session()
        

        # Load JSON data
        customer_df = spark.read \
            .option("minPartitions", 4) \
            .json(file_path_crm) \
            .select(
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
        
        # Convert Spark DataFrame to Pandas for Power BI compatibility
        pandas_df = customer_df.toPandas()

        # Convert DataFrame to dictionary format for JSON response
        data = pandas_df.to_dict(orient="records")
        
        return {"status": "success", "data": data}
    
    except Exception as e:
        logging.error(f"Error processing CRM data: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
