import snowflake.connector
import pandas as pd  # Fixed syntax error here
from datetime import datetime
import os
import random

# Snowflake connection details
snowflake_config = {
    "account": "ADIDJNN-GD31320",
    "user": "LOCHANA",
    "password": "Abcdefgh123456@",  # Set via GitHub secrets
    "database": "EXPORT_DB",
    "schema": "EXPORT_SCHEMA",
    "warehouse": "COMPUTE_WH",
    "role": "ACCOUNTADMIN"
}

# Connect to Snowflake
conn = snowflake.connector.connect(**snowflake_config)
cursor = conn.cursor()

# Create export_data table if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS EXPORT_DB.EXPORT_SCHEMA.export_data (
        country STRING,
        year INTEGER,
        month STRING,
        qty_kg FLOAT,
        value_rs FLOAT,
        price_per_kg FLOAT
    )
""")

# Generate dummy data
countries = ["USA", "India", "China", "UK", "Brazil"]
months = ["January", "February", "March", "April", "May"]
dummy_data = [
    (
        random.choice(countries),
        random.randint(2020, 2023),
        random.choice(months),
        round(random.uniform(1000, 10000), 2),  # qty_kg
        round(random.uniform(50000, 500000), 2),  # value_rs
        round(random.uniform(50, 100), 2)  # price_per_kg
    )
    for _ in range(50)  # 50 rows of dummy data
]

# Insert dummy data into export_data
insert_dummy_query = """
    INSERT INTO export_data (country, year, month, qty_kg, value_rs, price_per_kg)
    VALUES (%s, %s, %s, %s, %s, %s)
"""
cursor.executemany(insert_dummy_query, dummy_data)
conn.commit()
print("Dummy data inserted into export_data")

# Read data from export_data
query = """
    SELECT country, year, month, qty_kg, value_rs, price_per_kg
    FROM export_data
"""
cursor.execute(query)
df = pd.DataFrame(cursor.fetchall(), columns=["country", "year", "month", "qty_kg", "value_rs", "price_per_kg"])

# Add timestamp for simulation
df["timestamp"] = datetime.now()

# Process data (group by 5-minute window and country)
df["window_start"] = df["timestamp"].dt.floor("5min")
processed_df = (df.groupby(["window_start", "country"])
               .agg({
                   "qty_kg": ["mean", "max", "min"],
                   "value_rs": "mean",
                   "country": "count"
               })
               .reset_index())
processed_df.columns = ["window_start", "country", "avg_qty", "max_qty", "min_qty", "avg_value", "record_count"]

# Create window column as a string for Snowflake VARIANT
processed_df["window"] = processed_df["window_start"].apply(lambda x: f"{{'start': '{x}', 'end': '{x + pd.Timedelta(minutes=5)}'}}")
processed_df = processed_df[["window", "country", "avg_qty", "max_qty", "min_qty", "avg_value", "record_count"]]

# Create PROCESSED_STREAM_DATA table if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS EXPORT_DB.EXPORT_SCHEMA.PROCESSED_STREAM_DATA (
        window VARIANT,
        country STRING,
        avg_qty FLOAT,
        max_qty FLOAT,
        min_qty FLOAT,
        avg_value FLOAT,
        record_count BIGINT
    )
""")

# Write results to PROCESSED_STREAM_DATA
data_to_insert = [tuple(row) for row in processed_df.itertuples(index=False)]
insert_query = """
    INSERT INTO PROCESSED_STREAM_DATA (window, country, avg_qty, max_qty, min_qty, avg_value, record_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""
cursor.executemany(insert_query, data_to_insert)
conn.commit()

# Close connection
cursor.close()
conn.close()

print("Data processed and written to Snowflake successfully")
