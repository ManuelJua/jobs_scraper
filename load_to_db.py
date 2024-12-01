import os
from sqlalchemy import create_engine,text
from dotenv import load_dotenv
import pandas as pd
import datetime

load_dotenv()

def get_existent_data():
    print("Querying database...")
    # Load and process your data here
    conection_string=os.getenv('DATABASE_URL')
    engine=create_engine(conection_string)
    with engine.connect() as conn:
        result=conn.execute(text("""SELECT * FROM jobs;"""))
        existent_database=pd.DataFrame(result)
    return existent_database

def retrieve_addresses(df):
    location_df=pd.read_csv('location.csv')
    df=df.merge(location_df,how='left',on='location')
    df.to_csv('prueba.csv',index=False)
    print(df['latitude'].isna().value_counts())
    print(df.head())
    return df

def geolocate_addresses(df):
    pass


def main():
    existent_database=get_existent_data()
    print(f"reed_jobs_{datetime.date.today()}.parquet")
    new_data=pd.read_parquet(f"reed_jobs_{datetime.date.today()}.parquet")
    df=new_data.merge(existent_database,on='id',how='left',indicator=True).pipe(
        lambda df:df[df['_merge'] == 'left_only']
    ).pipe(
        lambda df:df.drop(columns=['_merge',
                                   'job_title_y',
                                   'salary_y',
                                   'job_url_y',
                                   'publication_date_y',
                                   'expiration_date_y',
                                   'description_y',
                                   'employer_name_y',
                                   'aplications_y',
                                    'location',
                                    'latitude',
                                    'longitude'
                                   ])
    ).pipe(
        lambda df:df.rename(columns={
            'job_title_x':'job_title',
            'location_name':'location',
            'salary_x':'salary',
            'job_url_x':'job_url',
            'publication_date_x':'publication_date',
            'expiration_date_x':'expiration_date',
            'description_x':'description',
            'employer_name_x':'employer_name',
            'aplications_x':'aplications'
        })
    )
    df=retrieve_addresses(df)
    df=geolocate_addresses(df)
start=datetime.datetime.now()
main()
finish=datetime.datetime.now()
print(f"Script duration {finish-start}")

