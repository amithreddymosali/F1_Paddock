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
    df = pd.json_normalize(races)
    return df

def fetch_all_paginated(endpoint, json_path_list):
    all_items = []
    limit = 100
    offset = 0

    while True:
        url = f"https://api.jolpi.ca/ergast/f1/{endpoint}.json?limit={limit}&offset={offset}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch {endpoint}: {response.status_code}")

        data = response.json()
        
        items = data
        for key in json_path_list:
            items = items.get(key, {})

        if not items:
            break

        all_items.extend(items)
        offset += limit

    return pd.json_normalize(all_items)

def fetch_drivers():
    # paginate to get all drivers
    return fetch_all_paginated("drivers", ["MRData", "DriverTable", "Drivers"])

def fetch_constructors():
    # paginate to get all constructors
    return fetch_all_paginated("constructors", ["MRData", "ConstructorTable", "Constructors"])

def fetch_circuits():
    # paginate to get all circuits
    return fetch_all_paginated("circuits", ["MRData", "CircuitTable", "Circuits"])


def fetch_results(season=2024):
    import requests
    import pandas as pd

    # First, fetch the season schedule to get all rounds
    schedule_url = f"https://api.jolpi.ca/ergast/f1/{season}.json"
    schedule_resp = requests.get(schedule_url)
    if schedule_resp.status_code != 200:
        raise Exception(f"Failed to fetch schedule: {schedule_resp.status_code}")

    schedule_data = schedule_resp.json()
    races = schedule_data["MRData"]["RaceTable"]["Races"]

    all_results = []

def fetch_qualifying_results(season=2024):
    import requests
    import pandas as pd

    schedule_url = f"https://api.jolpi.ca/ergast/f1/{season}.json"
    schedule_resp = requests.get(schedule_url)
    if schedule_resp.status_code != 200:
        raise Exception(f"Failed to fetch schedule: {schedule_resp.status_code}")

    schedule_data = schedule_resp.json()
    races = schedule_data["MRData"]["RaceTable"]["Races"]

    all_qualifying = []

    for race in races:
        round_num = race["round"]
        qualifying_url = f"https://api.jolpi.ca/ergast/f1/{season}/{round_num}/qualifying.json"
        qualifying_resp = requests.get(qualifying_url)
        if qualifying_resp.status_code != 200:
            print(f"Warning: No qualifying data for round {round_num}")
            continue

        qualifying_data = qualifying_resp.json()
        try:
            qualifying_results = qualifying_data["MRData"]["RaceTable"]["Races"][0].get("QualifyingResults", [])
        except (IndexError, KeyError):
            qualifying_results = []

        for qres in qualifying_results:
            qres["race_round"] = round_num
            all_qualifying.append(qres)

    df = pd.json_normalize(all_qualifying)
    return df


    # Loop through each race round to fetch results for that round
    for race in races:
        round_num = race["round"]
        results_url = f"https://api.jolpi.ca/ergast/f1/{season}/{round_num}/results.json"
        results_resp = requests.get(results_url)
        if results_resp.status_code != 200:
            print(f"Warning: Failed to fetch results for round {round_num}")
            continue

        results_data = results_resp.json()
        race_results = results_data["MRData"]["RaceTable"]["Races"][0].get("Results", [])
        for res in race_results:
            res["race_round"] = round_num
            all_results.append(res)

    df = pd.json_normalize(all_results)
    return df



if __name__ == "__main__":
    schedule_df = fetch_f1_schedule()
    print(schedule_df[["round", "raceName", "Circuit.circuitName", "date"]].head())




