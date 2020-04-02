from mongoengine import *


class Stats(EmbeddedDocument):
    last_updated = DateTimeField(required=True)
    tested = IntField(required=False)
    confirmed = IntField(required=True)
    deaths = IntField(required=True)


class Country(Document):
    country = StringField(required=True)
    alpha2Code = StringField(max_length=2, required=True)
    lat = FloatField(required=True)
    lon = FloatField(required=True)
    population = IntField(required=False)
    area = FloatField(required=False)
    stats = ListField(EmbeddedDocumentField(Stats, required=True))


class State(Document):
    state = StringField(required=True)
    stateAbbr = StringField(max_length=2, required=True)
    lat = FloatField(required=True)
    lon = FloatField(required=True)
    population = IntField(required=False)
    area = FloatField(required=False)
    stats = ListField(EmbeddedDocumentField(Stats, required=True))


class County(Document):
    state = StringField(required=True)
    stateAbbr = StringField(max_length=2, required=True)
    county = StringField(require=True)
    fips = IntField(required=True)
    lat = FloatField(required=True)
    lon = FloatField(required=True)
    population = IntField(required=True)
    area = FloatField(required=True)
    hospitals = IntField(required=False)
    hospital_beds = IntField(required=False)
    medium_income = FloatField(required=True)
    stats = ListField(EmbeddedDocumentField(Stats, required=True))


def main():
    # connect(host=config("MONGODB_CONNECTION_STAGING_URI"))

    # county = County(state="Michigan",
    #                 stateAbbr="MI",
    #                 county="Deer Field",
    #                 fips=12345,
    #                 lat=12.34567,
    #                 lon=89.01234,
    #                 stats=Stats(
    #                     last_updated="2020-03-31",
    #                     confirmed=12345,
    #                     deaths=67890,
    #                     )
    #                 )
    # county.save()

    # country = Country(country="South Korea",
    #             alpha2Code="SK",
    #             lat=12.34567,
    #             lon=89.01234,
    #             stats=Stats(
    #                 last_updated="2020-03-31",
    #                 confirmed=12345,
    #                 deaths=67890,
    #                 )
    #             )
    # country.save()

    # for county in County.objects:
    #     print(county.state)

    # disconnect()

    pass


if __name__ == "__main__":
    main()
