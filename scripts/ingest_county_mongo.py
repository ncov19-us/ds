from datetime import datetime
from decouple import config
import pandas as pd
from utils import *
import requests
from typing import Dict
from state_county_model_mongo import *
import random


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


def wrangle_county(confirmed, deaths, info):
    confirmed = confirmed[confirmed["province_state"].isin(STATES_MAP.keys())]
    deaths = deaths[deaths["province_state"].isin(STATES_MAP.keys())]
    info = info[info["state"].isin(STATES_MAP.values())]

    confirmed = confirmed[~confirmed["fips"].isna()]
    deaths = deaths[~deaths["fips"].isna()]

    confirmed = confirmed.drop(86, axis=0)
    confirmed["fips"] = confirmed["fips"].astype(int)
    deaths = deaths.drop(86, axis=0)
    deaths["fips"] = deaths["fips"].astype(int)

    info.at[1581, "fips_plotly"] = deaths[deaths["admin2"] == "Oglala Lakota"]["fips"]
    info.at[2355, "fips_plotly"] = deaths[
        (deaths["admin2"] == "Bedford") & (deaths["province_state"] == "Virginia")
    ]["fips"]
    info = info.drop(74, axis=0)
    info["fips_plotly"] = info["fips_plotly"].astype(int)

    info_columns = [
        "fips_plotly",
        "hospital_count",
        "bed_count",
        "median_household_income_2018",
        "censusarea",
    ]
    deaths_and_info = deaths.merge(
        info[info_columns], left_on="fips", right_on="fips_plotly", how="left"
    )
    columns_to_drop = ["uid", "iso2", "iso3", "code3", "country_region", "combined_key"]
    deaths_and_info.drop(columns=columns_to_drop, inplace=True)

    deaths_and_info.at[2828, "hospital_count"] = deaths_and_info[
        (deaths_and_info["admin2"] == "Bedford")
        & (deaths_and_info["province_state"] == "Virginia")
    ]["hospital_count"].max()
    deaths_and_info.at[2828, "bed_count"] = deaths_and_info[
        (deaths_and_info["admin2"] == "Bedford")
        & (deaths_and_info["province_state"] == "Virginia")
    ]["bed_count"].max()
    deaths_and_info = deaths_and_info.drop_duplicates("fips")

    confirmed = confirmed.reset_index().drop(["index"], axis=1)
    deaths_and_info = deaths_and_info.reset_index().drop(["index"], axis=1)

    list_of_fips_plotly_nans = deaths_and_info[deaths_and_info["fips_plotly"].isna()][
        "admin2"
    ].tolist()
    confirmed = confirmed[~confirmed["admin2"].isin(list_of_fips_plotly_nans)]
    deaths_and_info = deaths_and_info[
        ~deaths_and_info["admin2"].isin(list_of_fips_plotly_nans)
    ]

    assert confirmed["fips"].iloc[222] == deaths_and_info["fips"].iloc[222]
    assert confirmed["fips"].iloc[444] == deaths_and_info["fips"].iloc[444]
    assert confirmed["fips"].iloc[666] == deaths_and_info["fips"].iloc[666]
    assert confirmed["fips"].iloc[3131] == deaths_and_info["fips"].iloc[3131]

    return (confirmed, deaths_and_info)


def ingest_county():
    """ingestion script for county level data"""
    confirmed = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
    deaths = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
    info = "https://raw.githubusercontent.com/ncov19-us/ds/master/population-income-and-hospitals/county-level-hospital-population-and-income-data.csv"

    confirmed = read_df(confirmed)
    deaths = read_df(deaths)
    info = read_df(info)

    confirmed, deaths_and_info = wrangle_county(confirmed, deaths, info)
    deaths_and_info = deaths_and_info.fillna(0)

    connect(host=MONGO_CONNECTION_URI)

    try:
        County.drop_collection()
        print(f"[DEBUG] Dropped collection")
    except Exception as ex:
        print(f"[ERROR] Can't drop collection - {ex}")

    # Make sure the no. of counties match on both
    assert confirmed.shape[0] == deaths_and_info.shape[0]

    # Get number of counties
    num_counties = confirmed.shape[0]

    # Get the dates from confirmed columns
    dates = confirmed.columns.to_list()[11:]

    # have to use iloc otherwise code breaks by straight indexing.
    for i in range(num_counties):
        stats = []

        # Make sure counties are the same
        print(
            f"[DEBUG] Counties {confirmed['admin2'].iloc[i]} == {deaths_and_info['admin2'].iloc[i]}"
        )
        assert confirmed["fips"].iloc[i] == deaths_and_info["fips"].iloc[i]

        for j in range(len(dates)):

            last_updated = datetime.strptime(dates[j], "%m/%d/%y")
            cc = confirmed[dates[j]].iloc[i]
            dc = deaths_and_info[dates[j]].iloc[i]
            s = Stats(last_updated=last_updated, confirmed=cc, deaths=dc,)
            stats.append(s)

        county = County(
            state=confirmed["province_state"].iloc[i],
            stateAbbr=STATES_MAP[confirmed["province_state"].iloc[i]],
            county=confirmed["admin2"].iloc[i],
            fips=confirmed["fips"].iloc[i],
            lat=confirmed["lat"].iloc[i],
            lon=confirmed["long"].iloc[i],
            population=deaths_and_info["population"].iloc[i],
            area=deaths_and_info["censusarea"].iloc[i],
            hospitals=deaths_and_info["hospital_count"].iloc[i],
            hospital_beds=deaths_and_info["bed_count"].iloc[i],
            medium_income=deaths_and_info["median_household_income_2018"].iloc[i],
            stats=stats,
        )
        print(
            f"County({county.state}, {county.stateAbbr}, {county.county}, {county.fips}, {county.lat}, {county.lon}, {county.population}, {county.area}, {county.hospitals}, {county.hospital_beds}, {county.medium_income})"
        )
        county.save()

    disconnect()


def main():
    ingest_county()


if __name__ == "__main__":
    main()
