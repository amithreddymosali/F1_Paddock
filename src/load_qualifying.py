import psycopg2
from config.db_config import DB_CONFIG

def insert_qualifying_into_db(df):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Optional: clear existing qualifying data to avoid duplicates
    cur.execute("TRUNCATE TABLE qualifying RESTART IDENTITY;")

    for idx, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO qualifying (
                    race_round, driver_id, constructor_id, position,
                    q1, q2, q3
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (
                int(row.get("race_round")),
                row.get("Driver.driverId"),
                row.get("Constructor.constructorId"),
                int(row.get("position")) if row.get("position") else None,
                row.get("Q1"),
                row.get("Q2"),
                row.get("Q3")
            ))
            conn.commit()
        except Exception as e:
            print(f"Error inserting row {idx}: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    print("✅ Data inserted into qualifying table.")

if __name__ == "__main__":
    from src.jolpica_ingest import fetch_qualifying_results

    df_qualifying = fetch_qualifying_results()
    print(df_qualifying.head())

    insert_qualifying_into_db(df_qualifying)

