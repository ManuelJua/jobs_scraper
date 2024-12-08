import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import datetime
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)


def get_existent_data() -> pd.DataFrame:
    logging.info("Querying database...")
    # Load and process your data here
    conection_string = os.getenv('DATABASE_URL')
    engine = create_engine(conection_string)
    with engine.connect() as conn:
        result = conn.execute(text("""SELECT j.id, j.job_title, j.location,
                                  j.salary, j.job_url,
                                 j.publication_date, j.expiration_date,
                                 j.description, j.employer_name,
                                 j.aplications, c.latitude, c.longitude
                                      FROM jobs j JOIN coordinates c 
                                 on j.location=c.location;"""))
        existent_database = pd.DataFrame(result)
    return existent_database


def read_new_data() -> pd.DataFrame:
    logging.info("Reading new data")
    file_list = [file for file in os.listdir(".") if (
        file.endswith(".parquet") & file.startswith("reed_jobs"))]
    if len(file_list) == 1:
        df = pd.read_parquet(file_list[0])
    return df

def prepare_data_to_upload(df:pd.DataFrame) -> list:
    df=df.astype({'expiration_date':'datetime64[ns]'})
    records=df.head(20).replace(to_replace=[np.nan,pd.NA],value=None).to_dict('records')
    return records

def upload_data(records:list):
    logging.info("Starting to upload data")
    conection_string = os.getenv('DATABASE_URL')
    engine = create_engine(conection_string, pool_pre_ping=True)
    
    with engine.connect() as conn:
        result = conn.execute(text("""INSERT INTO jobs (id, job_title, location,
                                                    salary, job_url,
                                                    publication_date, expiration_date,
                                                    description, employer_name,
                                                    aplications)
                                    VALUES (:id, :job_title, :location,
                                                    :salary, :job_url,
                                                    :publication_date, :expiration_date,
                                                    :description, :employer_name,
                                                    :aplications)
                                 ON CONFLICT (id) DO NOTHING"""),records)
        conn.commit()
        logging.info("Insert finished")


def main():
    df = read_new_data()
    records=prepare_data_to_upload(df)
    upload_data(records)


start = datetime.datetime.now()
main()
finish = datetime.datetime.now()
print(f"Script duration {finish-start}")
