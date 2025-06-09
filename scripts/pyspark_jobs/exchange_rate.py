import requests
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("API_KEY")
SYMBOLS = ["EUR/USD", "JPY/USD"]

# Correct Snowflake config keys
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_EXCHANGE_curr_SCHEMA"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE", ""),
}

print("Snowflake URL:", SNOWFLAKE_CONFIG["account"])
print("Snowflake User:", SNOWFLAKE_CONFIG["user"])
print("Snowflake Password Set:", SNOWFLAKE_CONFIG["password"] is not None)

def get_price(symbol):
    url = f"https://api.twelvedata.com/price?symbol={symbol}&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()
    if "price" in data:
        return float(data["price"])
    else:
        raise Exception(f"Failed to fetch price for {symbol}: {data}")

if __name__ == "__main__":
    rates = {symbol: get_price(symbol) for symbol in SYMBOLS}
    
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cs = conn.cursor()
    
    try:
        cs.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_CONFIG["schema"]}.exchange_rates (
                id INTEGER AUTOINCREMENT PRIMARY KEY,
                symbol STRING,
                price FLOAT,
                fetched_at TIMESTAMP,
                CONSTRAINT unique_symbol_timestamp UNIQUE (symbol, fetched_at)
            )
        """)

        for symbol, price in rates.items():
            try:
                cs.execute(f"""
                    INSERT INTO {SNOWFLAKE_CONFIG["schema"]}.exchange_rates (symbol, price, fetched_at)
                    VALUES (%s, %s, %s)
                """, (symbol, price, datetime.utcnow()))
            except ProgrammingError as e:
                if "unique_symbol_timestamp" in str(e):
                    print(f"Duplicate entry for {symbol} at current time, skipping insert.")
                else:
                    raise
        conn.commit()
    finally:
        cs.close()
        conn.close()

    print("Exchange rates saved to Snowflake:", rates)
