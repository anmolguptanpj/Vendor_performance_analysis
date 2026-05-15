import gdown
import pandas as pd
import glob
import os
import shutil
import time
import logging
from sqlalchemy import create_engine


logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s",
    filemode="a"
)

def download_dataset():
    start = time.time()
    folder_url = "https://drive.google.com/drive/folders/1NEPUQIMhbquKzjcD40R4aKxyo2nv7QhW?usp=sharing"
    output_dir = "my_dataset_folder"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    gdown.download_folder(
        url=folder_url,
        output=output_dir,
        quiet=False
    )
    end=time.time()
    total_time=(end-start)/60
    logging.info('Download Complete')
    logging.info(f'\nTotal Time Takem:{total_time} minutes')

engine = create_engine("sqlite:///inventory.db")

def ingest_db(df,table_name,engine):
    df.to_sql(table_name,con = engine, if_exists="replace",index=False)

def load_raw_data():
    start = time.time()
    for file in os.listdir('my_dataset_folder'):
        if '.csv' in file:
            df=pd.read_csv('my_dataset_folder/'+file)
            logging.info(f'Ingesting{file} in db')
            ingest_db(df,file[:-4],engine)
    end=time.time()
    total_time=(end-start)/60
    logging.info('Ingestion Complete')
    logging.info(f'\nTotal Time Takem:{total_time} minutes')

if __name__ == '__main__':
    download_dataset()
    load_raw_data()


