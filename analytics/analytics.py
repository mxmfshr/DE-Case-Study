from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

import datetime
import math
import json
import pandas as pd

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here
def parse_lat_lon(location_string):
    location_json = json.loads(location_string)
    latitude = location_json['latitude']
    longitude = location_json['longitude']
    return pd.Series([latitude, longitude])

def parse_time(unixtime):
    unixtime = int(unixtime)
    dttm = datetime.datetime.fromtimestamp(unixtime)
    return dttm

def calculate_distance(w):
    if len(w) < 2: return 0
    
    EARTH_RADIUS_KM = 6371

    lat1, lat2 = w['lat'].apply(float).values
    lon1, lon2 = w['lon'].apply(float).values
    dist = (
        EARTH_RADIUS_KM *
        math.acos(
            math.sin(lat1) *
            math.sin(lat2) + math.cos(lat1) *
            math.cos(lat2) *
            math.cos(lon2 - lon1)
        )
    )
    return dist

def format_data(df):
    df[['lat', 'lon']] = df['location'].apply(lambda x: parse_lat_lon(x))
    df['dttm'] = df['time'].apply(lambda x: parse_time(x))
    df['dt'] = df['dttm'].apply(lambda x: x.date())
    df['hour'] = df['dttm'].apply(lambda x: x.hour)
    df = df.drop(['time', 'location'], axis=1)
    return df

def get_per_hour_stats(df):
    stats = (
        df
        .groupby(['device_id', 'dt', 'hour'])
        .agg({
            'device_id': 'count',
            'temperature': 'max'
        })
        .rename(columns={'device_id': 'cnt'})
        .reset_index()
    )
    return stats

def get_dists(devices_data):
    devices_dists = pd.DataFrame(columns=['device_id', 'dt', 'hour', 'distance'])
    
    slices = devices_data[['device_id', 'dt', 'hour']].drop_duplicates().values
    for device_id, dt, hour in slices:
        device_hour_slice = (
            devices_data
            [
                (devices_data['device_id'] == device_id) &
                (devices_data['dt'] == dt) &
                (devices_data['hour'] == hour)
            ]
            .sort_values('dttm')
        )

        total_distance = 0

        for w in device_hour_slice[['lat', 'lon']].rolling(window=2):
            total_distance += calculate_distance(w)
        total_distance = round(total_distance, 2)

        devices_dists.loc[len(devices_dists)] = [device_id, dt, hour, total_distance]
    
    return devices_dists

SOURCE_TABLE = 'devices'

devices_data = pd.read_sql(SOURCE_TABLE, psql_engine)
devices_data = format_data(devices_data)

devices_stats = get_per_hour_stats(devices_data)
devices_dists = get_dists(devices_data)
result = (
    devices_stats
    .merge(
        devices_dists,
        on=['device_id', 'dt', 'hour']
    )
)

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)

with mysql_engine.begin() as connection:
    result.to_sql('device_stats', con=connection, if_exists='replace', index=False)
print(f"Loaded {len(result)} rows")