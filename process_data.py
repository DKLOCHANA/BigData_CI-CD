
pip install snowflake-connector-python pandas

import snowflake.connector
import pandas as pd
from datetime import datetime
import os
snowflake_config["password"] = os.getenv("SNOWFLAKE_PASSWORD")

# Snowflake connection details
snowflake_config = {
    "account": "ADIDJNN-GD31320",
    "user": "LOCHANA",
    "password": "Abcdefgh123456@",  # Will be injected via GitHub secrets
    "database": "EXPORT_DB",
    "schema": "EXPORT_SCHEMA",
    "warehouse": "COMPUTE_WH",
    "role": "ACCOUNTADMIN"  # Replace with your actual role
}

# Connect to Snowflake
conn = snowflake.connector.connect(**snowflake_config)
cursor = conn.cursor()

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

# Write results to PROCESSED_STREAM_DATA
processed_df["window"] = processed_df["window_start"].apply(lambda x: f"{{'start': '{x}', 'end': '{x + pd.Timedelta(minutes=5)}'}}")
processed_df = processed_df[["window", "country", "avg_qty", "max_qty", "min_qty", "avg_value", "record_count"]]

# Convert to list of tuples for Snowflake insertion
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
