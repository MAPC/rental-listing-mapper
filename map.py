#!/usr/bin/env python3

import json
from sys import exit
from os import environ, path
from datetime import datetime
from datetime import date
from dateutil.relativedelta import *

import sqlalchemy
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

def longmap(record):
    if 'lng' in record:
        return float(record['lng'])
    else:
        return float(record['Longitude'])


def latmap(record):
    if 'lat' in record:
        return float(record['lat'])
    else:
        return float(record['Latitude'])


if 'MAPPER_YEAR' in environ:
    YEAR = int(environ['MAPPER_YEAR'])
else:
    print("Using current year for YEAR")
    YEAR = datetime.now().strftime('%Y')

DATE_RANGES = {
    1: '[{year}-01-01, {year}-03-31]'.format(year=YEAR),
    2: '[{year}-04-01, {year}-06-30]'.format(year=YEAR),
    3: '[{year}-07-01, {year}-09-30]'.format(year=YEAR),
    4: '[{year}-10-01, {year}-12-31]'.format(year=YEAR)
}

if 'MAPPER_QUARTER' in environ and int(environ['MAPPER_QUARTER']) in [1, 2, 3, 4]:
    RANGE = DATE_RANGES[int(environ['MAPPER_QUARTER'])]
elif 'MAPPER_MONTH' in environ:
    RANGE = '[{year}-{month}-01, {next_month}-01)'.format(
        year=YEAR, 
        month=int(environ['MAPPER_MONTH']),
        next_month=(date(YEAR, int(environ['MAPPER_MONTH']), 1) + relativedelta(months=+1)).strftime("%Y-%m")
    )
else:
    print("Getting data from last month. Otherwise you must set MAPPER_QUARTER in the environment, as 1, 2, 3, or 4")
    RANGE = '[{year}-{month}-01, {year}-{next_month}-01)'.format(
        year=YEAR,
        month=int(datetime.now().strftime('%m'))-1,
        next_month=datetime.now().strftime('%m')
    )

print("Reading raw listings from DB...")
engine = sqlalchemy.create_engine('postgresql://{}:{}@{}:{}/{}'.format(environ['DB_USER'], environ['DB_PASSWORD'], environ['DB_HOST'], environ['DB_PORT'], environ['DB_NAME']))
df = pd.read_sql_query(sqlalchemy.text('SELECT * FROM listings WHERE \'{range}\'::tsrange @> last_seen'.format(range=RANGE)), engine)

print("Mapping data to output format...")
df.rename(columns={'posting_date': 'post_at'}, inplace=True)

df = df.dropna(subset=['payload'])
df['longitude'] = df['payload'].apply(longmap)
df['latitude'] = df['payload'].apply(latmap)
df = df[df['latitude'] > 37]

mapped = df[['uid', 'ask', 'bedrooms', 'title', 'address', 'post_at', 'created_at', 'updated_at', 'source_id', 'survey_id', 'latitude', 'longitude']]

print(f"Writing to {environ['OUT_FILE_PATH']}{environ['OUT_FILE_NAME']}...")
mapped.to_csv(path.join(environ['OUT_FILE_PATH'], environ['OUT_FILE_NAME']), index=False, header=False)
print(f"Writing to {environ['OUT_TABLE_NAME']} table...")
mapped.to_sql(environ['OUT_TABLE_NAME'], engine, if_exists='append', index=False, chunksize=1000)

print("Done.")
