import os
import datetime
import logging
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ReedProcessor import Database, FileManager, DataPreparer

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
    absolute_path=os.path.abspath(os.path.join(os.path.dirname(__file__), 'data_to_upload.csv'))
    df_to_upload.to_csv(absolute_path,na_rep="",float_format="%.0f",index=False)
    logging.info("Starting to upload data")
    logging.info(absolute_path)
    db.copy_from_file_to_db(csv_file_path=absolute_path)
    logging.info("Data upload finished")
    for file_name in os.listdir():
        if file_name.endswith(".parquet"):
            os.remove(file_name)
            logging.info(f"{file_name} removed")
    

start = datetime.datetime.now()
main()
finish = datetime.datetime.now()
print(f"Script duration {(finish-start).seconds} seconds")
