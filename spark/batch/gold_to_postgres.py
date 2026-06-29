import os
from pathlib import Path
from dotenv import load_dotenv
from spark.common.spark_session import create_spark_session

# 1. Go up exactly two levels from /spark/batch/ to find the root folder .env
root_path = Path(__file__).resolve().parents[2]
env_file = root_path /"infra" /"docker"/".env"

print(f"Looking for .env file at: {env_file}")

if env_file.exists():
    load_dotenv(dotenv_path=env_file,override=True)
else:
    print("⚠️ Warning: .env file not found at project root! Trying standard fallback.")
    load_dotenv()

# 2. Extract database parameters
pg_user = os.getenv("POSTGRES_USER", "stock_admin")
pg_pass = os.getenv("POSTGRES_PASSWORD")  # Default password if .env is not found or empty
# 3. Ultimate Safety Fallback: If your .env still isn't reading, 
# look inside your docker-compose.yml to see your password and paste it here as a backup string
if not pg_pass:
    print("⚠️ .env password empty. Falling back to default 'stock_password'...")
    pg_pass = "stock_password" 

# Initialize your spark session
print(f"DEBUG CREDENTIALS ---> User: {pg_user} | Password: {pg_pass}")

spark = create_spark_session()


GOLD_PATH = "s3a://gold/daily_stock_summary"
df = spark.read.parquet(GOLD_PATH)

print("\nGold Data Preview:\n")
df.show()

# 3. Fire the secure pipeline dump
jdbc_url = "jdbc:postgresql://localhost:5432/stock_market"

print("Writing data rows safely to PostgreSQL Data Warehouse...")
print("=" * 50)
print("JDBC URL :", jdbc_url)
print("USER     :", pg_user)
print("PASSWORD :", repr(pg_pass))
print("LENGTH   :", len(pg_pass) if pg_pass else None)
print("=" * 50)
(
    df.write
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", "fact_stock_summary")
    .option("user", pg_user)  # Passes the exact user from your environment
    .option("password", pg_pass)  # Passes the exact password from your environment
    .option("driver", "org.postgresql.Driver")
    .mode("append")
    .save()
)

print("\n Gold Data Loaded Into PostgreSQL Successfully!")
spark.stop()
