import pandas as pd
import psycopg2
from config.db_config import DB_CONFIG
from src.jolpica_ingest import fetch_f1_schedule


def insert_into_db(df):
    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Insert data row by row
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO races (round, race_name, circuit_name, race_date)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (round) DO NOTHING;
        """, (int(row["round"]), row["raceName"], row["Circuit.circuitName"], row["date"]))

    # Save and close
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Data inserted into races table.")

if __name__ == "__main__":
    df = fetch_f1_schedule()
    insert_into_db(df)
