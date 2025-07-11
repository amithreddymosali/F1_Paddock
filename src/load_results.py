import pandas as pd
import psycopg2
from config.db_config import DB_CONFIG
from src.jolpica_ingest import fetch_results

def safe_int(val):
    if val is None:
        return None
    try:
        v = float(val)
        if pd.isna(v):
            return None
        return int(v)
    except:
        return None

def safe_float(val):
    if val is None:
        return None
    try:
        v = float(val)
        if pd.isna(v):
            return None
        return v
    except:
        return None

def insert_into_db(df):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE results RESTART IDENTITY;")  # Clear table before insert
    
    for idx, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO results (
                    race_round, driver_id, constructor_id, position, points, status,
                    time_millis, time_text, fastestlap_rank, fastestlap_lap, fastestlap_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                safe_int(row.get("race_round")),
                row.get("Driver.driverId"),
                row.get("Constructor.constructorId"),
                safe_int(row.get("position")),
                safe_float(row.get("points")),
                row.get("status"),

                safe_int(row.get("Time.millis")),
                row.get("Time.time"),
                safe_int(row.get("FastestLap.rank")),
                safe_int(row.get("FastestLap.lap")),
                row.get("FastestLap.Time.time")
            ))
            conn.commit()
        except Exception as e:
            print(f"Error inserting row {idx}: {e}")
            conn.rollback()
    
    cur.close()
    conn.close()
    print("✅ Data inserted into results table.")

if __name__ == "__main__":
    df = fetch_results()
    print("\nResults per round:")
    print(df['race_round'].value_counts().sort_index())
    insert_into_db(df)
