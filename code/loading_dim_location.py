import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import tqdm
from bs4 import BeautifulSoup
import requests
import urllib.request

'''
location_key INTEGER PRIMARY KEY,
longitude NUMERIC(9,6) CHECK (longitude BETWEEN -180 AND 180), -- ###.######
latitude NUMERIC(9,6) CHECK (latitude BETWEEN -90 AND 90), -- ###.######
nearest_intersection TEXT,
floor_level SMALLINT CHECK (floor_level > 0),
postal_code VARCHAR(6), -- LNLNLN
dissemination_area SMALLINT
'''

def get_dissemination_area(df):
    url = 'https://www12.statcan.gc.ca/census-recensement/2021/dp-pd/prof/search-recherche/results-resultats.cfm?Lang=E&SearchText='+df[:3]+'+'+df[4:]
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')
    if len(soup.find_all("details"))>=7:
        return soup.find_all("details")[6].find("a").decode_contents()
    else:
        return np.nan

pd.set_option('display.max_columns', None)

# Loading the raw csv file
df = pd.read_csv("../raw_data/fire_incidents_data.csv")

# Taking the columns that are relevant to the location dimension
df = df[['Longitude', 'Latitude', 'Intersection', 'Level_Of_Origin']]

# Removing rows with missing data
df = df[df['Longitude'].notna()]
df = df[df['Latitude'].notna()]
df = df[df['Intersection'].notna()]

# If no level of origin is provided, we assume it started on level 1
df['Level_Of_Origin'] = df['Level_Of_Origin'].fillna(1)

# Get the unique locations based on longitude and latitude
unique_locations = df.drop_duplicates(subset=['Longitude', 'Latitude'])

unique_locations = unique_locations.head(50)
# Generating the location key from the first four decimals of lat and long
unique_locations['Location_key'] = (((unique_locations['Latitude']-43)*10000).astype(int).astype(str)+((unique_locations['Longitude']+79)*-10000).astype(int).astype(str)).astype(int)

# Searching for postal code by lat and long
unique_locations["geom"] = unique_locations["Latitude"].map(str)+","+unique_locations["Longitude"].map(str)
locator = Nominatim(user_agent="ldunn084@uottawa.ca", timeout = 10)
rgeocode = RateLimiter(locator.reverse, min_delay_seconds = 1, max_retries=5)
tqdm.tqdm.pandas()
unique_locations["Address"] = unique_locations["geom"].progress_apply(rgeocode)
unique_locations["Postal_Code"] = unique_locations["Address"].progress_apply(lambda loc: loc.raw["address"]["postcode"]
                                                                             if (loc and "postcode" in loc.raw["address"].keys() and len(loc.raw["address"]["postcode"])==7)
                                                                             else np.nan)

# Remove rows with missing postal codes
unique_locations = unique_locations[unique_locations['Postal_Code'].notna()]

# Remove the geom and Address columns
unique_locations = unique_locations[['Location_key','Longitude','Latitude','Intersection','Level_Of_Origin','Postal_Code']]

# Finding the dissemination areas from postal code
unique_locations["Dissemination_area"] = unique_locations["Postal_Code"].progress_apply(get_dissemination_area)

# Remove rows with missing dissemination data
unique_locations = unique_locations[unique_locations['Dissemination_area'].notna()]

# Print to csv
unique_locations.to_csv('../sample_dimension_data/postal_codes.csv',sep=',',encoding='utf-8')
