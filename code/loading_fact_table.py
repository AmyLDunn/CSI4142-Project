import pandas as pd
from datetime import date
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import tqdm
from bs4 import BeautifulSoup
import requests
import urllib.request

'''
location_key INTEGER FOREIGN KEY dim_location(location_key),
date_key INTEGER FOREIGN KEY dim_date(date_key),
weather_key INTEGER FOREIGN KEY dim_weather(weather_key),
demographics_key INTEGER FOREIGN KEY dim_demographics(demographics_key),
fire_ward_key INTEGER FOREIGN KEY dim_fire_station(fire_station_key),
response_time SMALLINT CHECK (response_time >= 0),
damage_cad NUMERIC(11,2) CHECK (damage_cad >= 0),
casualties SMALLINT CHECK (casualties >= 0),
people_displaced SMALLINT CHECK (people_displaced >= 0),
people_rescued SMALLINT CHECK (people_rescued >= 0),
responding_personel SMALLINT CHECK (responding_personel >= 0),
responding_apparatus SMALLINT CHECK (responding_apparatus >= 0),
possible_cause causes,
sprinkler_stystem sprinkler,
smoke_system smoke_alarm,
fire_system fire_alarm
'''

def calculate_response_time(df):
    alarm_time = datetime.strptime(df['TFS_Alarm_Time'], "%Y-%m-%dT%H:%M:%S")
    clear_time = datetime.strptime(df["Last_TFS_Unit_Clear_Time"], "%Y-%m-%dT%H:%M:%S")
    return str(clear_time - alarm_time)

def get_possible_cause(df):
    code = int(df['Possible_Cause'][:2])
    if code in [1, 2, 3, 4]:
        return 'intentional'
    return 'unintentional'

def get_dissemination_area(df):
    url = 'https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/search-recherche/results-resultats.cfm?Lang=E&SearchText='+df[:3]+'+'+df[4:]
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    if len(soup.find_all("details"))>=7:
        return soup.find_all("details")[6].find("a").decode_contents()
    else:
        return np.nan
    
df = pd.read_csv("../raw_data/fire_incidents_data.csv")
df = df[df['Longitude'].notna()]
df = df[df['Latitude'].notna()]
df = df[df['Last_TFS_Unit_Clear_Time'].notna()]
df['Estimated_Dollar_Loss'] = df['Estimated_Dollar_Loss'].fillna(0)
df['Possible_Cause'] = df['Possible_Cause'].fillna('99 - Undetermined')

df = df.head(20)
facts = pd.DataFrame()
facts['location_key'] = (((df['Latitude']-43)*10000).astype(int).astype(str)+((df['Longitude']+79)*-10000).astype(int).astype(str)).astype(int)
facts['date_key'] = df["TFS_Alarm_Time"].str.replace("-","").str.replace("T","").str.replace(":","").astype('int64')
# facts['weather_key'] =

# Loading the dissemination area key
locator = Nominatim(user_agent="ldunn084@uottawa.ca", timeout = 10)
rgeocode = RateLimiter(locator.reverse, min_delay_seconds = 1, max_retries = 5)
tqdm.tqdm.pandas()
facts['geom'] = df["Latitude"].map(str)+","+df["Longitude"].map(str)
facts['address'] = facts['geom'].progress_apply(rgeocode)
facts['postal_code'] = facts['address'].progress_apply(lambda loc: loc.raw["address"]["postcode"]
                                                        if (loc and "postcode" in loc.raw["address"].keys() and len(loc.raw["address"]["postcode"])==7)
                                                        else np.nan)
facts = facts[facts['postal_code'].notna()]
facts['dissemination_area'] = facts['postal_code'].progress_apply(get_dissemination_area)
facts = facts[facts['dissemination_area'].notna()]
facts['year'] = df['TFS_Alarm_Time'].apply(lambda alarmTime: int(alarmTime[:4]))
facts['dissemination_year'] = facts["year"].apply(lambda thisyear: 2011 if thisyear <= 2011 else (2016 if thisyear <= 2016 else 2021))
facts['demographics_key'] = (facts['dissemination_year'].astype('str')+facts['dissemination_area'].astype('str')).astype('int64')
facts.drop(columns = ['geom','address', 'postal_code', 'dissemination_area', 'year', 'dissemination_year'])

# facts['fire_ward_Key'] = 
facts['response_time'] = df.apply(calculate_response_time, axis = 1)
facts['damage_cad'] = df['Estimated_Dollar_Loss']
facts['casualties'] = df['Civilian_Casualties']
facts['people_displaced'] = df['Estimated_Number_Of_Persons_Displaced']
facts['people_rescued'] = df['Count_of_Persons_Rescued']
facts['responding_personel'] = df['Number_of_responding_personnel'].astype('int')
facts['responding_apparatus'] = df['Number_of_responding_apparatus'].astype('int')
facts['possible_cause'] = df.apply(get_possible_cause, axis = 1)
# facts['sprinkler_system'] = 
# facts['smoke_system'] =
# facts['fire_system'] = 
print(facts)
