from geopy.adapters import AioHTTPAdapter
from geopy.geocoders import Nominatim
import asyncio
from time import sleep
import pandas as pd

class GetCoordinatesNominatim:
    """Class to handle geocoding using Nominatim."""
    
    def __init__(self):
        """Initialize the Nominatim geocoder with a user agent."""
        self.add_location_geocoded = {}
        self.df:pd.DataFrame
        self.location_df:pd.Series

    def _open_location_file(self, location_file_name:str):
        """Open the location file as a DataFrame."""
        file_extension = location_file_name.split('.')[-1]
        if file_extension == 'csv':
            self.location_df = pd.read_csv(location_file_name)
        elif file_extension == 'parquet':
            self.location_df = pd.read_parquet(location_file_name)
        else:
            raise ValueError("Unsupported file format: {}".format(file_extension))

    def _save_location_dataframe(self):
        """Save the updated location DataFrame to a new CSV file."""
        dict_df = pd.DataFrame(self.add_location_geocoded).T.reset_index()
        dict_df.columns = ['locationName', 'latitude', 'longitude']
        combined_df = pd.concat([self.location_df, dict_df], ignore_index=True)
        combined_df.to_csv('location_new.csv', index=False)

    async def _async_geocode_address(self, location:str):
        """Geocode a single address."""
        try:
            async with Nominatim(
                user_agent="jobs_scraper",
                adapter_factory=AioHTTPAdapter,
                ) as geolocator:
                geocode_result = await geolocator.geocode(location, timeout=None)
                if geocode_result:
                    # Extract latitude and longitude from the geocode result
                    self.add_location_geocoded[location] = (geocode_result.latitude, geocode_result.longitude)
                    print(location,geocode_result.latitude,geocode_result.longitude)
                    self._save_location_dataframe()
        except Exception as e:
            print(f"Error geocoding location {location}: {e}")

    async def _async_geocode_dataframe(self):
        """Geocode all addresses in the DataFrame with missing lat/lon."""
        unique_locations_missing = self.df[self.df['latitude'].isna()]['locationName'].unique()
        tasks=[asyncio.create_task(self._geocode_address(location)) for location in unique_locations_missing]
        await asyncio.gather(*tasks)
            
    def _geocode_address(self, location:str):
        """Geocode a single address."""
        try:
            geolocator=Nominatim(user_agent="jobs_scraper")
        
            geocode_result = geolocator.geocode(location, timeout=None)
            if geocode_result:
                # Extract latitude and longitude from the geocode result
                self.add_location_geocoded[location] = (geocode_result.latitude, geocode_result.longitude)
                print(location,geocode_result.latitude,geocode_result.longitude)
                self._save_location_dataframe()
            else:
                print(f"Coordinates not found for {location}")
        except Exception as e:
            print(f"Error geocoding location {location}: {e}")

    def _geocode_dataframe(self):
        """Geocode all addresses in the DataFrame with missing lat/lon."""
        unique_locations_missing = self.df[self.df['latitude'].isna()]['locationName'].unique()
        for location in unique_locations_missing:
            self._geocode_address(location)
      
    def async_geocode_dataframe(self, location_file_name:str, df:pd.DataFrame):
        """Public method to geocode addresses and save results."""
        self._open_location_file(location_file_name)
        self.df=df
        asyncio.run(self._async_geocode_dataframe())
    
    def geocode_dataframe(self, location_file_name:str, df:pd.DataFrame):
        """Public method to geocode addresses and save results."""
        print("Starting geocoding...")
        self._open_location_file(location_file_name)
        self.df=df
        self._geocode_dataframe()

