from datetime import datetime
from decouple import config
import pandas as pd
from utils import *
import requests
from typing import Dict
from state_county_model_mongo import *
import random

COUNTRY_INFO = config("COUNTRY_INFO")

# Switch to production once testing is done
MONGO_CONNECTION_URI = config("MONGODB_CONNECTION_STAGING_URI")


def read_df(url):
    import string

    df = pd.read_csv(url)
    df.columns = df.columns.str.lstrip(string.punctuation)
    df.columns = df.columns.str.rstrip(string.punctuation)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_")
    return df


def get_country_info(alpha2Code: str) -> Dict:
    try:
        if not (len(alpha2Code) == 2):
            raise ValueError(f"Incorrect {alpha2Code}")
        response = requests.get(COUNTRY_INFO + alpha2Code)
        response.raise_for_status()
        json_data = response.json()
        data = {"population": json_data["population"], "area": json_data["area"]}
    except Exception as ex:
        print(f"[ERROR]: {ex}")
        data = {"population": 0, "area": 0}
    return data


def wrangle_country(df):
    df = read_df(df)
    try:
        df.loc[df["country/region"] == "US", "country/region"] = "United States"
        print(df[df["country/region"] == "United States"])
    except Exception as ex:
        print(f"[ERROR] {ex}")
    df = df[df["country/region"].isin(COUNTRY_DICT.values())]
    df = df.groupby("country/region").sum()
    df = df.sort_index().reset_index()
    return df


def ingest_country():
    """ingestion script for country level data"""
    confirmed_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
    deaths_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"

    print(f"[DEBUG] Wrangling confirmed")
    confirmed = wrangle_country(confirmed_url)
    print(f"[DEBUG] Wrangling deaths")
    deaths = wrangle_country(deaths_url)
    print(f"[DEBUG] Confirmed shape: {confirmed.shape}, Deaths shape: {deaths.shape}")

    print(confirmed[confirmed["country/region"] == "United States"])
    print(deaths[deaths["country/region"] == "United States"])

    # sanity check

    # Test 1
    assert confirmed.shape == deaths.shape

    # Test 2
    rand1 = random.randint(0, confirmed.shape[0])
    print(f"[DEBUG] Testing index {rand1}")
    print(confirmed["country/region"][rand1])
    print(deaths["country/region"][rand1])
    assert confirmed["country/region"][rand1] == deaths["country/region"][rand1]

    # Test 3
    rand2 = random.randint(0, deaths.shape[0])
    print(f"[DEBUG] Testing index {rand2}")
    print(confirmed["country/region"][rand2])
    print(deaths["country/region"][rand2])
    assert confirmed["country/region"][rand2] == deaths["country/region"][rand2]

    # Test 4
    rand3 = random.randint(0, confirmed.shape[0])
    print(f"[DEBUG] Testing index {rand3}")
    print(confirmed["country/region"][rand3])
    print(deaths["country/region"][rand3])
    assert confirmed["country/region"][rand3] == deaths["country/region"][rand3]

    num_countries = confirmed.shape[0]
    dates = confirmed.columns.to_list()[3:]

    # Connect to MongoDB
    connect(host=MONGO_CONNECTION_URI)

    # Drop Colleciton if exists
    try:
        Country.drop_collection()
        print(f"[DEBUG] Dropped collection")
    except:
        pass
    for i in range(num_countries):
        print(f"[DEBUG] Inserting {confirmed['country/region'][i]}")

        stats = []
        for j in range(len(dates)):
            s = Stats(
                last_updated=datetime.strptime(dates[j], "%m/%d/%y"),
                confirmed=confirmed[dates[j]][i],
                deaths=deaths[dates[j]][i],
            )
            stats.append(s)

        country_name = confirmed["country/region"][i]
        alpha2Code = REVERSE_COUNTRY_MAP[country_name]
        lat = confirmed["lat"][i]
        long = confirmed["long"][i]
        country_info = get_country_info(alpha2Code)
        population = country_info["population"]
        area = country_info["area"]
        print(
            f"[DEBUG] Country({country_name}, {alpha2Code}, {lat}, {long}, {population}, {area})"
        )

        country = Country(
            country=country_name,
            alpha2Code=alpha2Code,
            lat=lat,
            lon=long,
            population=population,
            area=area,
            stats=stats,
        )

        country.save()

    disconnect()


def main():
    ingest_country()


if __name__ == "__main__":
    main()
