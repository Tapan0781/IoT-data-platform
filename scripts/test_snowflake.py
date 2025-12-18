from snowflake.snowpark import Session
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("Testing Snowflake connection...")

try:
    # Create session
    session = Session.builder.configs({
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }).create()
    
    # Test query
    result = session.sql("SELECT CURRENT_VERSION(), CURRENT_USER(), CURRENT_DATABASE()").collect()
    
    print("✅ Connection successful!")
    print(f"Version: {result[0][0]}")
    print(f"User: {result[0][1]}")
    print(f"Database: {result[0][2]}")
    
    session.close()
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
