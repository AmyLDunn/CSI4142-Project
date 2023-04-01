#uncomment the following line of code if any of the following libraries are not installed locally
#!pip install psycopg2
#!pip install sqlalchemy
#!pip install pandas
#!pip install datetime
#!pip install geopy
#!pip install tqdm
#!pip install bs4
#!pip install requests
#!pip install urllib
#!pip install json

#importing the necessary 
import psycopg2
import sqlalchemy
import pandas as pd
from datetime import date
import json
from pandas import json_normalize



#################################################################################
#GETTING DEMOGRAPHICS DATA AND PREPARING DATAFRAME FOR LOADING INTO THE DATABASE#
#################################################################################
#getting data from the preprocessed csv file for 2022
df_date_2021=pd.read_csv('../raw_data/census_2021.csv')
df_date_2021= df_date_2021.drop(columns = ['Unnamed: 0'])
#getting the data fromt the preprocessed csv file for 2016
df_date_2016=pd.read_csv('../raw_data/census_2016.csv')
df_date_2016= df_date_2016.drop(columns = ['Unnamed: 0'])
df_date_2016.dissemination_area=df_date_2016.dissemination_area.astype('int64')
#getting the data fromt the preprocessed csv file for 2011
df_date_2011=pd.read_csv('../raw_data/census_2011.csv')
df_date_2011= df_date_2011.drop(columns = ['Unnamed: 0'])
#copy the median_household_income from the next year since 2011 census doesn't contain this columns
#as stated in our assumptions
df_date_2011= df_date_2011.drop(columns = ['median_household_income'])
df_date_2011=df_date_2011.merge(df_date_2016[['dissemination_area', 'median_household_income']], on = 'dissemination_area', how = 'left')
#reordering the rows to match the other dataframes
df_date_2011=df_date_2011[['demographics_key', 'dissemination_area','population','average_age','median_age',
                'total_dwellings','average_household_size','median_household_income',
                'mother_tongue_official_percentage','mother_tongue_unofficial_percentage','census_year']]
#combine the three census years together into one dataframe
frames = [df_date_2011, df_date_2016, df_date_2021]
result = pd.concat(frames)
#Removed the average age column as the average age was not reported in the 2011 census profilesà
#and we had the data from the 
result=result.drop(columns=['average_age'])
#removed the rows where the population through mother_tongue_unofficial was null
result=result.dropna(subset=['population','average_household_size','median_household_income',
                             'mother_tongue_official_percentage','mother_tongue_unofficial_percentage']).reset_index(drop = True) 
#convert demographics_key into an integer
result.demographics_key=result.demographics_key.astype('int64')
#convert population into an integer
result.population=result.population.astype('int64')
#convert date to an integer
result.census_year=result.census_year.astype('int64')
#convert total_dwellings to int
result.total_dwellings=result.total_dwellings.astype('int64')


#############################################################################
#GETTING LOCATION DATA AND PREPARING DATAFRAME FOR LOADING INTO THE DATABASE#
#############################################################################

#load the data
unique_locations=pd.read_csv("../raw_data/dim_location.csv")
#remove the pandas generated index row
unique_locations=unique_locations.drop(columns=["Unnamed: 0"])
#renamed the rows to match with the SQL names
unique_locations= unique_locations.rename(columns={'Location_key':'location_key','Longitude':'longitude','Latitude':'latitude',
'Intersection':'nearest_intersection','Level_Of_Origin':'floor_level','Postal_Code':'postal_code','Dissemination_area':'dissemination_area'})
#Basement are denoted with a B which I replaced with a negative sign since the database requires an smallint 
unique_locations['floor_level'] =unique_locations['floor_level'].apply(lambda x: x.replace("B", "-"))
#removes duplicates of the primary key in the database (precision is within 10m
unique_locations.drop_duplicates(subset='location_key', keep='first', inplace=True)
#For one row, the dissemination_area that was obtained was a region name instead so the row was removed from the dataframe
unique_locations=unique_locations[unique_locations['dissemination_area'] != 'North Battleford']


############################################################################
#GETTING WEATHER DATA AND PREPARING DATAFRAME FOR LOADING INTO THE DATABASE#
############################################################################

#create a dataframe
weather=pd.read_csv("../raw_data/Weather.csv")
#rename columns to be the same as the ones in the SQL code
weather= weather.rename(columns={'Key':'weather_key','TFS_Alarm_Time':'datetime',
                                          'Latitude':'latitude','Longitude':'longitude',
                                          'StartTime':'start_time' ,'EndTime':'end_time',
                                          'temp':'temperature', 'dwpt':'dewpoint',
                                          'rhum':'relative_humidity', 'prcp':'precipitaion',
                                          'snow':'snow','wdir':'wind_direction',
                                          'wspd':'wind_speed'
})
#remove any duplicates
weather.drop_duplicates(subset='weather_key', keep='first', inplace=True)


#########################################################################
#GETTING DATE DATA AND PREPARING DATAFRAME FOR LOADING INTO THE DATABASE#
#########################################################################

# Holiday dates obtained from https://www.statutoryholidays.com/
def getHolidayName(df_date):
    thisdate = date(df_date["year"], df_date["month"], df_date["day"])
    if thisdate == date(df_date["year"], 1, 1):
        return "New year's day"
    elif df_date["month"] == 2 and df_date["day_of_week"] == 1 and df_date["day"] >= 15 and df_date["day"] <= 21:
        return "Family day"
    elif thisdate in [date(2011, 4, 22), date(2012, 4, 6), date(2013, 3, 29),
                  date(2014, 4, 18), date(2015, 4, 3), date(2016, 3, 25),
                  date(2017, 4, 14), date(2018, 3, 30), date(2019, 4, 19),
                  date(2020, 4, 10), date(2021, 4, 2)]:
        return "Good friday"
    elif thisdate in [date(2011, 4, 24), date(2012, 4, 8), date(2013, 3, 31),
                  date(2014, 4, 20), date(2015, 4, 5), date(2016, 3, 27),
                  date(2017, 4, 16), date(2018, 4, 1), date(2019, 4, 21),
                  date(2020, 4, 12), date(2021, 4, 4)]:
        return "Easter sunday"
    elif thisdate == date(df_date["year"], 7, 1):
        return "Canada day"
    elif df_date["month"] == 8 and df_date["day_of_week"] == 1 and df_date["day"] <= 7:
        return "Civic holiday"
    elif df_date["month"] == 9 and df_date["day_of_week"] == 1 and df_date["day"] <= 7:
        return "Labour day"
    elif df_date["month"] == 10 and df_date["day_of_week"] == 1 and df_date["day"] >= 8 and df_date["day"] <=14:
        return "Thanksgiving"
    elif thisdate == date(df_date["year"], 12, 25):
        return "Christmas day"
    elif thisdate == date(df_date["year"], 12, 26):
        return "Boxing day"
    elif thisdate == date(df_date["year"], 2, 14):
        return "Valentines day"
    elif thisdate == date(df_date["year"], 3, 17):
        return "St. Patricks day"
    elif thisdate == date(df_date["year"], 10, 31):
        return "Halloween"
    
pd.set_option('display.max_columns', None)

# Loading the raw data
df_date = pd.read_csv("../raw_data/fire_incidents_data.csv")

# Grabbing the time column
df_date = df_date[['TFS_Alarm_Time']]
# Calculating the values for the columns
df_date["date_key"] = df_date["TFS_Alarm_Time"].str.replace("-","").str.replace("T","").str.replace(":","").astype('int64')
df_date["datetime"] = df_date["TFS_Alarm_Time"]
df_date["year"] = df_date["datetime"].apply(lambda thisdate: thisdate[0:4]).astype('int')
df_date["month"] = df_date["datetime"].apply(lambda thisdate: thisdate[5:7]).astype('int')
df_date["day"] = df_date["datetime"].apply(lambda thisdate: thisdate[8:10]).astype('int')
df_date["hour"] = df_date["datetime"].apply(lambda thisdate: thisdate[11:13]).astype('int')
df_date["minute"] = df_date["datetime"].apply(lambda thisdate: thisdate[14:16]).astype('int')
df_date["day_of_week"] = df_date["datetime"].apply(lambda thisdate: date(int(thisdate[0:4]), int(thisdate[5:7]), int(thisdate[8:10])).weekday()+1).astype('int')
df_date["weekend"] = df_date["day_of_week"].apply(lambda thisdate: True if thisdate >= 6 else False)
df_date["time_of_day"] = df_date["hour"].apply(lambda thishour: 'night' if thishour < 6 else ('morning' if thishour < 12 else ('afternoon' if thishour < 18 else 'evening')))
df_date["census_year"] = df_date["year"].apply(lambda thisyear: 2011 if thisyear <= 2011 else (2016 if thisyear <= 2016 else 2021))
df_date["holiday"] = df_date.apply(getHolidayName, axis = 1)
df_date["is_holiday"] = df_date["holiday"].apply(lambda thisholiday: True if thisholiday else False)

# Dropping the original column
df_date = df_date.drop(columns=["TFS_Alarm_Time"])

##############################################################################
#GETTING FIRE WARD DATA AND PREPARING DATAFRAME FOR LOADING INTO THE DATABASE#
##############################################################################

df = pd.read_csv("../raw_data/fire-station-locations.csv")
b = df["geometry"][0]

b = b.replace("\'", "\"")
b= b.replace("(", "[")
b= b.replace(")", "]")

# b = '{"type": "Point", "coordinates": [-79.2428700353868, 43.8239927252015]}'

c = json.loads(b)
df['geometry'] = df['geometry'].apply(lambda x: x.replace("\'", "\""))
df['geometry'] = df['geometry'].apply(lambda x: x.replace("(", "["))
df['geometry'] = df['geometry'].apply(lambda x: x.replace(")", "]"))
df['geometry'] = df['geometry'].apply(lambda x: json.loads(x))
dfLat = df['geometry'].map(lambda x: x['coordinates'][0])
dfLong = df['geometry'].map(lambda x: x['coordinates'][1])
df2=df.merge(dfLat.rename('Latitude'), left_index=True, right_index=True)
df2 = df2.merge(dfLong.rename('Longitude'), left_index=True, right_index=True)
df2 = df2.drop(columns=['_id','ADDRESS_POINT_ID','ADDRESS_NUMBER','LINEAR_NAME_FULL','CENTRELINE_ID','OBJECTID','YEAR_BUILD','PUBLIC_ED_OFFICE','FIRE_PREV_OFFICE','FIRE_OTHER', 'geometry'])
df2 = df2[df2["TYPE_DESC"] !="Admin"].reset_index()  #removes admin values, not a station and then numbers indexes correctly
unique = {}
stations = df2["WARD"]
for _, station in stations.items():
    added = False
    for wardNum in unique:
        if station == wardNum:
            unique[station] = unique[station]+1
            added = True
            
    if not added:
        unique[station] =1


total = 0
for wardNum in unique:
    total = total+unique[wardNum]
dfStation = pd.DataFrame.from_dict(unique, orient='index', columns=[ 'StationsInWard'])
dfStation = dfStation.reset_index()
dfStation = dfStation.rename(columns = {"index":"WardNumber"})
dfStation = dfStation.sort_values(by = ["WardNumber"])
dfStation.reset_index(inplace = True)
dfStation = dfStation.drop(columns = "index")
dfStation = dfStation.rename(columns={'WardNumber':'fire_ward_key','StationsInWard':'stations_in_ward'})


##########################################################################
#GETTING FACT TABLE AND PREPARING DATAFRAME FOR LOADING INTO THE DATABASE#
##########################################################################


# Loading raw data csv
facts = pd.read_csv("../raw_data/fact_table.csv")
facts = facts.drop(columns=["Unnamed: 0"])

#remove rows where the FK don't match the demographics_key
facts = facts[facts['demographics_key'].isin(result['demographics_key'])]

#remove rows where the FK don't match the weather_key
facts = facts[facts['weather_key'].isin(weather['weather_key'])]

#remove rows where the FK don't match the fire_ward_key
facts = facts[facts['fire_ward_key'].isin(dfStation['fire_ward_key'])]

#########################
#POPULATING THE DATABASE#
#########################

#Using postgres 13
#format: postgresql+psycopg2://username:password@host_address/database_name
engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:CSI4142@localhost:5432/fire_hazard')

#loads demographic dimension
result.to_sql(name='dim_demographics', con=engine,if_exists='append', index=False)
#loads the location dimension
unique_locations.to_sql(name='dim_location', con=engine,if_exists='append', index=False)
#loads the weather dimension
weather.to_sql(name='dim_weather', con=engine,if_exists='append', index=False)
#loads the date dimension
df_date.to_sql(name='dim_date', con=engine,if_exists='append', index=False)
#loads the fire ward dimension
dfStation.to_sql(name='dim_fire_ward', con=engine,if_exists='append', index=False)
#loads the fire incidents fact table 
facts.to_sql(name='fact_fire_incidents', con=engine,if_exists='append', index=False)
