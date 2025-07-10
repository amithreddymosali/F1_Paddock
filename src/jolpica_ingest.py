import requests
import json
import pandas as pd

def fetch_f1_schedule(season=2024):
    url = f"https://api.jolpi.ca/ergast/f1/{season}.json"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")

    data = response.json()
    races = data["MRData"]["RaceTable"]["Races"]

    # Convert to DataFrame
    df = pd.json_normalize(races)
    return df

if __name__ == "__main__":
    schedule_df = fetch_f1_schedule()
    print(schedule_df[["round", "raceName", "Circuit.circuitName", "date"]].head())

