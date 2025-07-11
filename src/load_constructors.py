import psycopg2
from config.db_config import DB_CONFIG
from src.jolpica_ingest import fetch_constructors

def insert_into_db(df):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO constructors (constructor_id, name, nationality)
            VALUES (%s, %s, %s)
            ON CONFLICT (constructor_id) DO NOTHING;
        """, (
            row.get("constructorId"),
            row.get("name"),
            row.get("nationality")
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {len(df)} constructors into constructors table.")

if __name__ == "__main__":
    df = fetch_constructors()
    insert_into_db(df)

from src.jolpica_ingest import fetch_constructors

df = fetch_constructors()
print(df.head())
