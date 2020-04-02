from datetime import datetime
from decouple import config
import pandas as pd
from utils import *
import requests
from typing import Dict
from state_county_model_mongo import *
import random

COUNTRY_INFO = "https://restcountries.eu/rest/v2/alpha/"


# Switch to production once testing is done
MONGO_CONNECTION_URI = config("MONGODB_CONNECTION_STAGING_URI")


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


def read_df(url):
    import string

    df = pd.read_csv(url)
    df.columns = df.columns.str.lstrip(string.punctuation)
    df.columns = df.columns.str.rstrip(string.punctuation)
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_")
    return df


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
        status = Country.drop_collection()
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


def ingest_county():
    """ingestion script for county level data"""
    confirmed = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
    deaths = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
    info = "https://raw.githubusercontent.com/ncov19-us/ds/master/population-income-and-hospitals/county-level-hospital-population-and-income-data.csv"

    confirmed = pd.read_csv(confirmed)
    deaths = pd.read_csv(deaths)
    info = pd.read_csv(info)

    confirmed = confirmed[confirmed["Province_State"].isin(REVERSE_STATES_MAP.values())]
    deaths = deaths[deaths["Province_State"].isin(REVERSE_STATES_MAP.values())]

    print(f"Total Number of counties confirmed {confirmed['Admin2'].unique().shape}")
    print(f"Total Number of counties deaths {deaths['Admin2'].unique().shape}")
    print(f"Total Number of counties info has {info.shape}")
    # Admin2,Province_State,Country_Region
    dates = confirmed.columns.to_list()[11:]

    assert confirmed["Admin2"][37] == deaths["Admin2"][37]
    assert confirmed["Admin2"][1115] == deaths["Admin2"][1115]
    assert confirmed["Admin2"][1715] == deaths["Admin2"][1715]

    num_counties = confirmed.shape[0]
    # dates = confirmed.columns.to_list()[3:]

    connect(host=MONGO_CONNECTION_URI)

    # have to use iloc otherwise code breaks by straight indexing.
    for i in range(num_counties):

        stats = []
        for j in range(len(dates)):
            s = Stats(
                last_updated=datetime.strptime(dates[j], "%m/%d/%Y"),
                confirmed=confirmed[dates[j]].iloc[i],
                deaths=deaths[dates[j]].iloc[i],
            )
            stats.append(s)

        county = County(
            state=confirmed["Province_State"].iloc[i],
            stateAbbr=states_map[confirmed["Province_State"].iloc[i]],
            county=confirmed["Admin2"].iloc[i],
            fips=confirmed["FIPS"].iloc[i],
            lat=confirmed["Lat"].iloc[i],
            lon=confirmed["Long_"].iloc[i],
            population=deaths["Population"].iloc[i],
            area=0,
            hospitals=0,
            hospital_beds=0,
            medium_income=0,
            stats=stats,
        )

        county.save()

    count = 0
    for county in County.objects:
        print(county.county)
        count += 1

    print(f"Numbers of countries in dataset {num_counties}, ingested {count}")

    disconnect()


def main():
    ingest_country()
    # ingest_county()


if __name__ == "__main__":
    main()
