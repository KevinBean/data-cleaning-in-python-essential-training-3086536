"""
Load traffic.csv into "traffic" table in sqlite3 database.

Drop and report invalid rows.
- ip should be valid IP (see ipaddress)
- time must not be in the future
- path can't be empty
- status code must be a valid HTTP status code (see http.HTTPStatus)
- size can't be negative or empty

Report the percentage of bad rows. Fail the ETL if there are more than 5% bad rows
"""

import ipaddress
import sqlite3
import pandas as pd
from invoke import task

# http.HTTPStatus
from http import HTTPStatus
status_code = [a.value for a in HTTPStatus]

# ipaddress can use to validate an ip address


def validate_ip_address(ip_string):
    try:
        ip_object = ipaddress.ip_address(ip_string)
        return True
    except ValueError:
        return False

# load csv


def load_csv(csv_file):
    df = pd.read_csv(csv_file, parse_dates=['time'])
    return df

# validate data


def validate(df):
    # to validate an ipaddress use Regex, see following line
    # ip_mask = df['ip'].str.match("'\b((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.|$)){4}\b'")

    # use ".apply" to validate by a function
    ip_mask = df['ip'].apply(validate_ip_address)

    # to get the current time, use pd.Timestamp.now(), use strftime to transfer timestamp to string
    time_mask = df['time'] <= pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%S")

    # to find if a pandas DataFrame column is in a list, use ".isin"
    http_mask = df['status'].isin(status_code)

    # to validate size
    size_mask = df['size'] >= 0

    df_bad = df[~ip_mask | ~time_mask | ~http_mask | ~size_mask]

    if len(df_bad) > 0:
        raise ValueError(df_bad)


@task
def etl(ctx, csv_file):
    df = load_csv(csv_file)
    validate(df)

    db_file = f'traffic.db'
    conn = sqlite3.connect(db_file)
    df.to_sql('traffic', conn, index=False, if_exists='append')
