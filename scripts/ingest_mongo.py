from datetime import datetime
from decouple import config
import pandas as pd
from mongoengine import *
import pymongo
import pprint
from utils import *

class Stats(EmbeddedDocument):
    last_updated = DateTimeField(required=False)
    tested = IntField(required=False)
    confirmed = IntField(required=True)
    deaths = IntField(required=True)


class Country(Document):
    country = StringField(required=True)
    alpha2Code = StringField(max_length=2, required=True)
    lat = FloatField(required=True)
    lon = FloatField(required=True)
    population = IntField(required=False)
    area = IntField(required=False)
    stats = ListField(EmbeddedDocumentField(Stats, required=False))


class State(Document):
    state = StringField(required=True)
    stateAbbr = StringField(max_length=2, required=True)
    lat = FloatField(required=True)
    lon = FloatField(required=True)
    population = IntField(required=False)
    area = IntField(required=False)
    stats = ListField(EmbeddedDocumentField(Stats, required=False))


class County(Document):
    state = StringField(max_length=15, required=True)
    stateAbbr = StringField(max_length=2, required=True)
    county = StringField(max_length=100, require=True)
    fips = IntField(max_length=6, required=True)
    lat = FloatField(required=True)
    lon = FloatField(required=True)
    population = IntField(required=False)
    area = IntField(required=False)
    hospitals = IntField(required=False)
    hospital_beds = IntField(required=False)
    medium_income = IntField(required=False)
    stats = ListField(EmbeddedDocumentField(Stats, required=False))


def ingest_country():
    """ingestion script for country level data"""
    confirmed = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
    deaths = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"

    confirmed = pd.read_csv(confirmed)
    deaths = pd.read_csv(deaths)
    
    print(f"Total Number of Countries confirmed {confirmed['Country/Region'].unique().shape}")
    print(f"Total Number of Countries deaths {deaths['Country/Region'].unique().shape}")

    confirmed = confirmed[confirmed['Country/Region'].isin(COUNTRY_DICT.values())]
    deaths = deaths[deaths['Country/Region'].isin(COUNTRY_DICT.values())]

    confirmed = confirmed.groupby('Country/Region').sum()
    deaths = deaths.groupby('Country/Region').sum()
    confirmed = confirmed.sort_index().reset_index()
    deaths = deaths.sort_index().reset_index()

    # sanity check
    assert confirmed.shape == deaths.shape
    assert confirmed['Country/Region'][37] == deaths['Country/Region'][37]
    assert confirmed['Country/Region'][115] == deaths['Country/Region'][115]
    assert confirmed['Country/Region'][175] == deaths['Country/Region'][175]
    
    num_countries = confirmed.shape[0]
    dates = confirmed.columns.to_list()[3:]
    
    connect(host=config("MONGODB_CONNECTION_URI"))
    
    for i in range(num_countries):
        # print(confirmed['Country/Region'][i], confirmed['Lat'][i], confirmed['Long'][i])

        stats = []
        for j in range(len(dates)):
            s = Stats(
                last_updated = datetime.strptime(dates[j], "%m/%d/%y"),
                confirmed = confirmed[dates[j]][i],
                deaths = deaths[dates[j]][i],
            )
            stats.append(s)
            
        country = Country(
                    country = confirmed['Country/Region'][i],
                    alpha2Code = reverse_country_map[confirmed['Country/Region'][i]],
                    lat = confirmed['Lat'][i],
                    lon = confirmed['Long'][i],
                    stats = stats,
        )
        
        country.save()
        
    count = 0
    for country in Country.objects:
        print(country.country)
        count += 1

    print(f'Numbers of countries in dataset {num_countries}, ingested {count}')

    disconnect()




def ingest_county():
    """ingestion script for county level data"""
    confirmed = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"
    deaths = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"
    info = "https://raw.githubusercontent.com/ncov19-us/ds/master/population-income-and-hospitals/county-level-hospital-population-and-income-data.csv"
    confirmed = pd.read_csv(confirmed)
    deaths = pd.read_csv(deaths)

    confirmed = confirmed[confirmed['Province_State'].isin(REVERSE_STATES_MAP.values())]
    deaths = deaths[deaths['Province_State'].isin(REVERSE_STATES_MAP.values())]

    print(f"Total Number of counties confirmed {confirmed['Admin2'].unique().shape}")
    print(f"Total Number of counties deaths {deaths['Admin2'].unique().shape}")

    # Admin2,Province_State,Country_Region
    dates = confirmed.columns.to_list()[11:]

    assert confirmed['Admin2'][37] == deaths['Admin2'][37]
    assert confirmed['Admin2'][1115] == deaths['Admin2'][1115]
    assert confirmed['Admin2'][1715] == deaths['Admin2'][1715]
    
    num_counties = confirmed.shape[0]
    # dates = confirmed.columns.to_list()[3:]

    connect(host=config("MONGODB_CONNECTION_STAGING_URI"))

    # have to use iloc otherwise code breaks by straight indexing.
    for i in range(num_counties):

        stats = []
        for j in range(len(dates)):
            s = Stats(
                last_updated = datetime.strptime(dates[j], "%m/%d/%Y"),
                confirmed = confirmed[dates[j]].iloc[i],
                deaths = deaths[dates[j]].iloc[i],
            )
            stats.append(s)
            
        county = County(
                    state = confirmed['Province_State'].iloc[i],
                    stateAbbr = states_map[confirmed['Province_State'].iloc[i]],
                    county = confirmed['Admin2'].iloc[i],
                    fips = confirmed['FIPS'].iloc[i],
                    lat = confirmed['Lat'].iloc[i],
                    lon = confirmed['Long_'].iloc[i],
                    population = deaths['Population'].iloc[i],
                    area = 0,
                    hospitals = 0,
                    hospital_beds = 0,
                    medium_income = 0,
                    stats = stats,
        )
        
        county.save()
        
    count = 0
    for county in County.objects:
        print(county.county)
        count += 1

    print(f'Numbers of countries in dataset {num_counties}, ingested {count}')

    disconnect()



def main():
    ingest_country()
    # ingest_county()


if __name__ == "__main__":
    main()
