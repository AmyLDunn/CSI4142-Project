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
                 #LIMIT {number_rows};"""
print(query)
df = pd.read_sql_query(sql=text(query), con=engine.connect())
server.close()

print("\n")

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
create_header("Data summary")
print(df.describe())

print("\n")

create_header("Number of NAN values in each column")
print(df.isna().sum())
print("\nWe will replace all the NAN holiday value with the string 'Not holiday'")
# holiday is the only column with any null values.
# We will replace them with 0 as an arbitary placeholder
df['holiday'] = df['holiday'].fillna("Not holiday")

print("\n")

create_header("Type of values in each column")
print(df.dtypes)
print("\nWe will use one-hot encoding on the 'object' type columns - holiday and time-of-day")
# holiday and time_of_day are non-numerical types
# We will use one-hot encoding to separate these types
df = pd.get_dummies(df,
                    columns = ['holiday', 'time_of_day'],
                    prefix = ['holiday', 'time_of_day'])

print("\n")

create_header("Number of NAN values in each column after adjusting")
print(df.isna().sum())

print("\n")

create_header("Type of values in each column after encoding")
print(df.dtypes,'\n\n')

# We need to balance the output categories
create_header("Balancing the output categories")
print("The output category (status) is unbalanced.")
print("Before (0 = 'Bad', 1 = 'Good'):")
print(df['status'].describe())
numBad, numGood = df.status.value_counts()      #counts the number of good and bad samples
goodClass = df[df['status']==1]            #make a df of the rows with the good class
badClass = df[df['status']==0]              #make a df of the rows with the bad class

badDf = badClass.sample(numGood)                #take a sample of the bad class (~5000 in this case)

df = pd.concat([goodClass, badDf], axis =0) #join the samples to one df called undersample dataframe
print("\nAfter undersampling the 'Bad' outcomes:")
print(df['status'].describe())

print("\n")

# Now we will normalize all columns
for column_name in df.columns:
    df[column_name] = MinMaxScaler().fit_transform(np.array(df[column_name]).reshape(-1, 1))
create_header("Data summary after normalization")
print(df.describe())

# Split the data and target
y = df['status']
X = df.drop(columns=['status'])
X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)

# Creating the decision tree and training
decision_tree = DecisionTreeClassifier()
decision_tree_time = time.time()
decision_tree.fit(X_train, y_train)
decision_tree_time = time.time() - decision_tree_time

y_true = decision_tree.predict(X_test)
create_header("Decision tree - Classification report")
print(classification_report(y_true, y_test, target_names = ['Bad', 'Good']))
print("\nThe decision tree took "+str(decision_tree_time)+" seconds to train")

print("\n")

# Creating the gradient booster and training
gradient_boosting = GradientBoostingClassifier()
gradient_boosting_time = time.time()
gradient_boosting.fit(X_train, y_train)
gradient_boosting_time = time.time() - gradient_boosting_time

y_true = gradient_boosting.predict(X_test)
create_header("Gradient boosting - Classification report")
print(classification_report(y_true, y_test, target_names = ['Bad', 'Good']))
print("\nThe gradient boosting took "+str(gradient_boosting_time)+" seconds to train")

print("\n")

# Creating the random forest and training
random_forest = RandomForestClassifier()
random_forest_time = time.time()
random_forest.fit(X_train, y_train)
random_forest_time = time.time() - random_forest_time

y_true = random_forest.predict(X_test)
create_header("Random forest - Classification report")
print(classification_report(y_true, y_test, target_names = ['Bad', 'Good']))
print("\nThe random forest took "+str(random_forest_time)+" seconds to train")
