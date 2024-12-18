import os
from dotenv import load_dotenv
import datetime
import logging
from ReedProcessor import Database,FileManager,DataPreparer

load_dotenv()
logging.basicConfig(level=logging.INFO)

def main():
    connection_string = os.getenv('DATABASE_URL')
    db = Database(connection_string)
    file_manager = FileManager()
    data_preparer = DataPreparer()

    
    logging.info("Retrieving ids from database")
    text_query="""SELECT id FROM jobs;"""
    existent_data=db.query(query=text_query)
    logging.info("Reading new data")
    new_data = file_manager.get_parquet_file()
    df_to_upload=data_preparer.filter_new_rows(existent_db=existent_data,new_data=new_data)
    logging.info(f"{df_to_upload.shape[0]} new jobs available")
    logging.info("Cleaning data")
    df_to_upload=data_preparer.transform_data(df=df_to_upload)
    logging.info("Preparing data to upload")
    df_to_upload.to_csv('data_to_upload.csv',na_rep="",float_format="%.0f",index=False)
    logging.info("Starting to upload data")
    db.copy_from_file_to_db(csv_file='data_to_upload.csv')
    logging.info("Data upload finished")
    

start = datetime.datetime.now()
main()
finish = datetime.datetime.now()
print(f"Script duration {(finish-start).seconds} seconds")
