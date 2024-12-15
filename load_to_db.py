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


    logging.info("Reading new data")
    new_data = file_manager.get_parquet_file()

    logging.info("Preparing data to upload")
    records = data_preparer.prepare_to_records(new_data)

    logging.info("Starting to upload data")
    insert_query = """INSERT INTO jobs (id, job_title, location, salary, job_url, publication_date, expiration_date,
                                        description, employer_name, aplications)
                      VALUES (:id, :job_title, :location, :salary, :job_url, :publication_date, :expiration_date,
                              :description, :employer_name, :aplications)
                      ON CONFLICT (id) DO NOTHING"""
    chunks=[records[i:i + 100] for i in range(0, len(records), 100)]
    for i,chunk in enumerate(chunks):
        db.insert(insert_query, chunk)
        logging.info(f"{i} Insert finished")

start = datetime.datetime.now()
main()
finish = datetime.datetime.now()
print(f"Script duration {finish-start}")
