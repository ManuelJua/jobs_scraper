import os
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
import logging
from typing import List
import requests
import re
import asyncio
import aiohttp



logging.basicConfig(level=logging.INFO)


# Single Responsibility Principle - Separating concerns
class Database:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string,pool_pre_ping=True)

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
        df = df.astype({'expiration_date': 'datetime64[ns]'})
        records = df.replace(to_replace=[np.nan, pd.NA], value=None).to_dict('records')
        return records
    
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
        
        
        
    