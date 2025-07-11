import pandas as pd
import psycopg2
from config.db_config import DB_CONFIG
from src.jolpica_ingest import fetch_circuits

def insert_into_db(df):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO circuits (
                circuit_id, circuit_name, location, country
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (circuit_id) DO NOTHING;
        """, (
            row.get("circuitId"),
            row.get("circuitName"),
            row.get("Location.locality"),
            row.get("Location.country")
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data inserted into circuits table.")

if __name__ == "__main__":
    df = fetch_circuits()
    insert_into_db(df)

