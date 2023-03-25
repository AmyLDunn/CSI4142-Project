import pandas as pd
from datetime import date
'''
date_key INTEGER PRIMARY KEY,
datetime TIMESTAMP CHECK (datetime BETWEEN '2011-01-01T00:00:00' AND '2019-12-31T23:59:59'),
year SMALLINT CHECK (year BETWEEN 2011 AND 2019),
month SMALLINT CHECK (month BETWEEN 1 AND 12),
day SMALLINT CHECK (day BETWEEN 1 AND 31),
day_of_week SMALLINT CHECK (day_of_week BETWEEN 1 AND 7),
weekend BOOLEAN,
hour SMALLINT CHECK (hour BETWEEN 0 AND 23),
minute SMALLINT CHECK (minute BETWEEN 0 AND 59),
holiday VARCHAR(20),
is_holiday BOOLEAN,
time_of_day part_of_day,
census_year SMALLINT CHECK (census_year IN [2011, 2016, 2021])
'''

# Holiday dates obtained from https://www.statutoryholidays.com/
def getHolidayName(df):
    thisdate = date(df["year"], df["month"], df["day"])
    if thisdate == date(df["year"], 1, 1):
        return "New year's day"
    elif df["month"] == 2 and df["day_of_week"] == 1 and df["day"] >= 15 and df["day"] <= 21:
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
    elif thisdate == date(df["year"], 7, 1):
        return "Canada day"
    elif df["month"] == 8 and df["day_of_week"] == 1 and df["day"] <= 7:
        return "Civic holiday"
    elif df["month"] == 9 and df["day_of_week"] == 1 and df["day"] <= 7:
        return "Labour day"
    elif df["month"] == 10 and df["day_of_week"] == 1 and df["day"] >= 8 and df["day"] <=14:
        return "Thanksgiving"
    elif thisdate == date(df["year"], 12, 25):
        return "Christmas day"
    elif thisdate == date(df["year"], 12, 26):
        return "Boxing day"
    elif thisdate == date(df["year"], 2, 14):
        return "Valentine's day"
    elif thisdate == date(df["year"], 3, 17):
        return "St. Patrick's day"
    elif thisdate == date(df["year"], 10, 31):
        return "Halloween"
    
pd.set_option('display.max_columns', None)

# Loading the raw data
df = pd.read_csv("../raw_data/fire_incidents_data.csv")

# Grabbing the time column
df = df[['TFS_Alarm_Time']]
# Calculating the values for the columns
df["date_key"] = df["TFS_Alarm_Time"].str.replace("-","").str.replace("T","").str.replace(":","").astype('int64')
df["datetime"] = df["TFS_Alarm_Time"]
df["year"] = df["datetime"].apply(lambda thisdate: thisdate[0:4]).astype('int')
df["month"] = df["datetime"].apply(lambda thisdate: thisdate[5:7]).astype('int')
df["day"] = df["datetime"].apply(lambda thisdate: thisdate[8:10]).astype('int')
df["hour"] = df["datetime"].apply(lambda thisdate: thisdate[11:13]).astype('int')
df["minute"] = df["datetime"].apply(lambda thisdate: thisdate[14:16]).astype('int')
df["day_of_week"] = df["datetime"].apply(lambda thisdate: date(int(thisdate[0:4]), int(thisdate[5:7]), int(thisdate[8:10])).weekday()+1).astype('int')
df["weekend"] = df["day_of_week"].apply(lambda thisdate: True if thisdate >= 6 else False)
df["time_of_day"] = df["hour"].apply(lambda thishour: 'night' if thishour < 6 else ('morning' if thishour < 12 else ('afternoon' if thishour < 18 else 'evening')))
df["census_year"] = df["year"].apply(lambda thisyear: 2011 if thisyear <= 2011 else (2016 if thisyear <= 2016 else 2021))
df["holiday"] = df.apply(getHolidayName, axis = 1)
df["is_holiday"] = df["holiday"].apply(lambda thisholiday: True if thisholiday else False)

# Dropping the original column
df = df.drop(columns=["TFS_Alarm_Time"])

# Printing to csv
df.to_csv('../sample_dimension_data/dim_date.csv',sep=',',encoding='utf-8')

# save to database
import psycopg2
import sqlalchemy
engine = sqlalchemy.create_engine('postgresql+psycopg2://postgres:CSI4142@localhost:5432/fire_hazard')
df.to_sql(name='dim_date', con=engine,if_exists='append', index=False)
