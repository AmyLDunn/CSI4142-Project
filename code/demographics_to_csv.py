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

result.to_csv('../raw_data/demographics.csv',sep=',',encoding='utf-8')
