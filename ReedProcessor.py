import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import pandas as pd
import numpy as np
import logging
from typing import List
import requests
import re
import asyncio
import aiohttp
import io



logging.basicConfig(level=logging.INFO)


# Single Responsibility Principle - Separating concerns
class Database:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string,pool_pre_ping=True,connect_args={'connect_timeout': 10})

    def query(self, query: str) -> pd.DataFrame:
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return pd.DataFrame(result)

    def insert(self, query: str, records: List[dict]):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query), records)
                conn.commit()
        except Exception as e:
            logging.info(f"Connection failed: {e}")
    
    def single_insert(self, query: str,):
        try:
            with self.engine.connect() as conn:
                conn.execute(text(query))
                conn.commit()
        except Exception as e:
            logging.info(f"Connection failed: {e}")
    
    def copy_from_file_to_db(self,csv_file:str):
        file_path=f"/home/manuel/platzi/portfolio/current_projects/reed_scraper/{csv_file}"
        session= sessionmaker(self.engine)()
        cursor=session.connection().connection.cursor()
        with open(file_path,'r') as file:
            cursor.copy_expert(
                f"""COPY jobs(id, job_title, location, salary, job_url, publication_date, expiration_date,
            description, employer_name, aplications) FROM STDIN WITH (FORMAT CSV, HEADER)""",
            file
            )
            session.commit()
        

class FileManager:
    @staticmethod
    def get_parquet_file() -> pd.DataFrame:
        file_list = [file for file in os.listdir(".") if file.endswith(".parquet") and file.startswith("reed_jobs")]
        if len(file_list) == 1:
            return pd.read_parquet(file_list[0])
        return pd.DataFrame()

class DataPreparer:
    @staticmethod
    def prepare_to_records(df: pd.DataFrame) -> List[dict]:
        df['expiration_date'] = pd.to_datetime( df['expiration_date'],format="%d/%m/%Y" )
        df.salary=df.salary.astype('int64')
        records = df.replace(to_replace=[np.nan, pd.NA], value=None).to_dict('records')
        return records
    
    @staticmethod
    def filter_new_rows(existent_db:pd.DataFrame,new_data:pd.DataFrame) ->pd.DataFrame:
        merged_df=pd.merge(new_data,existent_db,on='id',how='outer',indicator=True)
        new_rows=merged_df[merged_df['_merge'] =='left_only' ].drop(columns='_merge')
        return new_rows
    @staticmethod
    def transform_data(df:pd.DataFrame) -> pd.DataFrame:
        df['expiration_date'] = pd.to_datetime( df['expiration_date'],format="%d/%m/%Y" )
        df=df.replace(to_replace=[np.nan, pd.NA], value=None)
        # Applies fillna first in so pandas allows to convert numbers to int64.
        # Then the -1 values are converted to None as the database takes them as null
        df[['salary','aplications']]=df[['salary','aplications']].fillna(-1).apply(np.int64).replace(to_replace=[-1],value=None)
        # Replaces all nan values for None, which are taken by the database as Null values
        df=df.replace(to_replace=[np.nan, pd.NA], value=None)
        
        
        
        
        return df
    
class JobAvailable:
    @staticmethod
    def check_is_available(url:str):
        try:
            response=requests.get(url,timeout=10)
            logging.info(response.status_code)
        except requests.exceptions.Timeout as e:
            print(e)
            return True
        if response.status_code==200:
            pattern=re.compile("""The following job is no longer available""")
            match=re.search(pattern,response.content.decode("utf-8"))
            if match:
                return False
            else:
                return True
        else:
            return True
        
    # async def async_check_is_available(self,url:str,client):
    #     try:
    #         async with client.get(url,timeout=10) as response:
    #             logging.info(response.status)
    #             if response.status==200:
    #                 pattern=re.compile("""The following job is no longer available""")
    #                 match=re.search(pattern,response.text("utf-8"))
    #                 logging.info(response.text("utf-8"))
    #                 if match:
    #                     return False
    #                 else:
    #                     return True
    #             else:
    #                 return True
    #     except Exception as e:
    #         logging.info(e)
    #         return True
        
        
        
    