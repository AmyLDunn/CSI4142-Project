from sshtunnel import SSHTunnelForwarder
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn import tree
from matplotlib import pyplot as plt
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
import time
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier

def create_header(text):
    print('**'+'*'*len(text)+'**')
    print('* '+' '*len(text)+' *')
    print('* '+text+' *')
    print('* '+' '*len(text)+' *')
    print('**'+'*'*len(text)+'**')

server = SSHTunnelForwarder(
    ssh_address=('174.114.0.126',8022),
    ssh_username='remote_user',
    ssh_password='remote',
    remote_bind_address=('174.114.0.126',5432))

server.start()

engine = create_engine('postgresql+psycopg2://service:service@localhost:'+str(server.local_bind_port)+'/postgres')

number_rows = 20
create_header("Query")
query = f"""SELECT dl.longitude,
                   dl.latitude,
                   dd.month,
                   dd.day,
                   dd.day_of_week,
                   dd.holiday,
                   dd.time_of_day,
                   dw.temperature,
                   dw.relative_humidity,
                   dw.precipitaion,
                   dw.snow,
                   dw.wind_direction,
                   dw.wind_speed,
                   dde.population,
                   dde.median_age,
                   dde.total_dwellings,
                   dde.average_household_size,
                   dde.median_household_income,
                   dde.mother_tongue_official_percentage,
                   dde.mother_tongue_unofficial_percentage,
                   dfw.stations_in_ward,
                   CASE
                     WHEN (ffi.casualties + ffi.people_displaced) >= 5 THEN 0
                     WHEN ffi.damage_cad >= 10000::numeric THEN 0
                     WHEN "substring"(ffi.response_time, "position"(ffi.response_time, ':'::text) + 1, 2)::integer >= 20 THEN 0
                     ELSE 1
                   END AS status
                 FROM fact_fire_incidents ffi
                 LEFT JOIN dim_location dl ON ffi.location_key = dl.location_key
                 LEFT JOIN dim_date dd ON ffi.date_key = dd.date_key
                 LEFT JOIN dim_weather dw ON ffi.weather_key = dw.weather_key
                 LEFT JOIN dim_demographics dde ON ffi.demographics_key = dde.demographics_key
                 LEFT JOIN dim_fire_ward dfw ON ffi.fire_ward_key = dfw.fire_ward_key"""

print(query)
df = pd.read_sql_query(sql=text(query), con=engine.connect())
server.close()


print("\n")

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
create_header("Data summary")

meanDf = df.describe().iloc[1]                                              #take average values
transposedMeanDf = meanDf.to_frame().T.reset_index().drop('index', axis=1)  #take the mean of every value as a series, then transform it so the data is horizontal
                                                                            #then reset index, drop the text mean
transposedMeanDf.insert(5, "holiday", [""])
transposedMeanDf.insert(6, "time_of_day", ["afternoon"])

repeatedMeanDf = transposedMeanDf.loc[transposedMeanDf.index.repeat(5)]     #copy value 5 times
repeatedMeanDf.iloc[0,7]=20                         
repeatedMeanDf.iloc[1,17]=60000 
repeatedMeanDf.iloc[2,15]=700
repeatedMeanDf.iloc[3,8]=80

print(repeatedMeanDf)
repeatedMeanDf.to_csv('sample_data.csv', mode='a', index=False, header=False)             #add it to sample_data.csv