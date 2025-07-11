import psycopg2
from config.db_config import DB_CONFIG
from src.jolpica_ingest import fetch_drivers
import pandas as pd

def insert_into_db(df):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        permanent_number = row.get("permanentNumber")
        if pd.isna(permanent_number):
            permanent_number = None
        else:
            permanent_number = int(permanent_number)

        cur.execute("""
            INSERT INTO drivers (
                driver_id, permanent_number, code, given_name, family_name, date_of_birth, nationality
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (driver_id) DO NOTHING;
        """, (
            row.get("driverId"),
            permanent_number,
            row.get("code"),
            row.get("givenName"),
            row.get("familyName"),
            row.get("dateOfBirth"),
            row.get("nationality")
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {len(df)} drivers into drivers table.")

if __name__ == "__main__":
    df = fetch_drivers()
    insert_into_db(df)
