from geopy.geocoders import Nominatim
from time import sleep
import pandas as pd
import logging
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)


def geocode_address(location:str):
        """Geocode a single address."""
        try:
            geolocator=Nominatim(user_agent="jobs_scraper")
            geocode_result = geolocator.geocode(location, timeout=None)
            if geocode_result:
                # Extract latitude and longitude from the geocode result
                logging.info(location,geocode_result.latitude,geocode_result.longitude)
                return (round(geocode_result.latitude,5),round( geocode_result.longitude,5))
                
            else:
                logging.info(f"Coordinates not found for {location}")
                return None
        except Exception as e:
            logging.info(f"Error geocoding location {location}: {e}")

def download_addresses():
    logging.info("Starting to download data")
    conection_string = os.getenv('DATABASE_URL')
    engine = create_engine(conection_string, pool_pre_ping=True)
    
    with engine.connect() as conn:
        addresses= conn.execute(text("""SELECT DISTINCT
                                        j.location
                                    FROM jobs j left join coordinates c
                                    on j.location=c.location
                                    where c.latitude is null;"""))
        
        addresses=map(lambda x: x[0],addresses)

        for address in addresses:
            coordinates=geocode_address(address)
            
            if coordinates:
                result= conn.execute(text("""INSERT INTO coordinates (location,
                                                    latitude,
                                                    longitude)
                                    VALUES (:location,:latitude,:longitude)
                                """),(address,coordinates[0],coordinates[1]))

                conn.commit()
                logging.info("Values {address} , {coordinates[0]} , {coordinates[1]} inserted into database")

download_addresses()


