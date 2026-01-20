#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='taxi_zone_lookup', help='Target table name')
@click.option('--url', default='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv', help='URL or local path to CSV file')
def run(user, password, host, port, db, table, url):
    """Ingest taxi zone lookup CSV into PostgreSQL database."""
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print(f"Reading CSV from: {url}")
    
    # Read the CSV file
    df = pd.read_csv(url)
    
    print(f"Loaded {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    
    # Display first few rows
    print("\nFirst 5 rows:")
    print(df.head())
    
    # Ingest to PostgreSQL
    print(f"\nIngesting data to table '{table}'...")
    df.to_sql(
        name=table,
        con=engine,
        if_exists='replace',
        index=False
    )
    
    print(f"Successfully ingested {len(df)} rows to table '{table}'")

if __name__ == '__main__':
    run()