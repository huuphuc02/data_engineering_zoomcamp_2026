#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import numpy as np

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='green_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading parquet')
def run(user, password, host, port, db, table, chunksize):
    """Ingest NYC taxi data into PostgreSQL database."""
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the entire parquet file
    df = pd.read_parquet(url)
    
    # Calculate number of chunks
    total_rows = len(df)
    num_chunks = (total_rows // chunksize) + (1 if total_rows % chunksize else 0)

    first = True

    # Process in chunks
    for i in tqdm(range(0, total_rows, chunksize), total=num_chunks, desc="Processing chunks"):
        df_chunk = df.iloc[i:i+chunksize]
        
        if first:
            df_chunk.head(0).to_sql(
                name=table,
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
            name=table,
            con=engine,
            if_exists='append'
        )

if __name__ == '__main__':
    run()