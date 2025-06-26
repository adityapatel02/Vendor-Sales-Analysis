import pandas as pd
import os
from sqlalchemy import create_engine, text

engine = create_engine('sqlite:///inventory.db')

def ingest_db(chunk, table_name, engine):
    chunk.to_sql(table_name, engine, if_exists='append', index=False)

for file in os.listdir('data'):
    if file.endswith('.csv'):
        table_name = file[:-4]  # remove .csv
        csv_path = os.path.join('data', file)
        
        print(f"Clearing table {table_name} (if it exists)...")
        try:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
                print(f"Table {table_name} dropped.")
        except Exception as e:
            print(f"Could not drop table {table_name}: {e}")
        
        print(f"Ingesting {file} into {table_name}...")

        try:
            for chunk in pd.read_csv(csv_path, chunksize=100000):
                ingest_db(chunk, table_name, engine)
                print(f"Inserted chunk of shape {chunk.shape} into {table_name}")
        except Exception as e:
            print(f"Failed to ingest {file}: {e}")
