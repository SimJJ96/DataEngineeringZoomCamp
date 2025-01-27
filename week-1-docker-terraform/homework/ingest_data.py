#!/usr/bin/env python
# coding: utf-8
import argparse
import os
import time
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_name = params.csv_name

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    if 'lpep_pickup_datetime' in df.columns:
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    if 'lpep_dropoff_datetime' in df.columns:    
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try: 
            t_start = time.time()
            
            df = next(df_iter)

            if 'lpep_pickup_datetime' in df.columns:
                df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            if 'lpep_dropoff_datetime' in df.columns:    
                df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            t_end = time.time()

            print('Inserted another chunk..., took %.3f second' %(t_end- t_start))
        except StopIteration:
            print("All chunks have been processed.")
            break

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgre')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table-name', help='name of table where we will write the results to')
    parser.add_argument('--csv-name', help='name of the csv file')

    args = parser.parse_args()
    main(args)

