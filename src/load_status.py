import requests
import psycopg2
from config.db_config import DB_CONFIG  # ✅ your config
from tqdm import tqdm

BASE_URL = "https://api.jolpi.ca/ergast/f1"

def fetch_unique_statuses():
    statuses = set()

    for year in tqdm(range(2000, 2025)):  # up to 2024
        for round_num in range(1, 23):
            url = f"{BASE_URL}/{year}/{round_num}/results.json"
            try:
                res = requests.get(url, timeout=10)
                data = res.json()
                races = data['MRData']['RaceTable']['Races']
                if races:
                    results = races[0].get('Results', [])
                    for r in results:
                        statuses.add(r['status'])
            except Exception:
                continue

    return list(statuses)

def insert_statuses_to_db(statuses):
    conn = psycopg2.connect(**DB_CONFIG)  # ✅ consistent with your results loader
    cur = conn.cursor()

    for status in statuses:
        try:
            cur.execute(
                "INSERT INTO status (status_text) VALUES (%s) ON CONFLICT (status_text) DO NOTHING;",
                (status,)
            )
        except Exception as e:
            print(f"Error inserting status '{status}': {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    statuses = fetch_unique_statuses()
    print(f"✅ Found {len(statuses)} unique statuses.")
    insert_statuses_to_db(statuses)
