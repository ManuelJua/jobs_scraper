from geopy.geocoders import Nominatim
from time import sleep
import pandas as pd

class GetCoordinatesNominatim:
    """Class to handle geocoding using Nominatim."""
    
    def __init__(self):
        """Initialize the Nominatim geocoder with a user agent."""
        self.app = Nominatim(user_agent='jobs')
        self.add_location_geocoded = {}

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

    def _geocode_address(self, location:str):
        """Geocode a single address."""
        try:
            geocode_result = self.app.geocode(location)
            if geocode_result:
                # Extract latitude and longitude from the geocode result
                self.add_location_geocoded[location] = (geocode_result.latitude, geocode_result.longitude)
                print(location,geocode_result.latitude,geocode_result.longitude)
                self._save_location_dataframe()
        except Exception as e:
            print(f"Error geocoding location {location}: {e}")

    def _geocode_dataframe(self, df:pd.DataFrame):
        """Geocode all addresses in the DataFrame with missing lat/lon."""
        unique_locations_missing = df[df['latitude'].isna()]['locationName'].unique()
        for location in unique_locations_missing:
            self._geocode_address(location)

    def geocode_dataframe(self, location_file_name:str, df:pd.DataFrame):
        """Public method to geocode addresses and save results."""
        self._open_location_file(location_file_name)
        self._geocode_dataframe(df)

# from geopy.geocoders import Nominatim
# from time import sleep
# import pandas as pd
# import asyncio
# from aiohttp import ClientSession

# class GetCoordinatesNominatim:
#     """Class to handle geocoding using Nominatim."""

#     def __init__(self):
#         """Initialize the Nominatim geocoder with a user agent."""
#         self.app = Nominatim(user_agent='jobs')
#         self.add_location_geocoded = {}

#     def _open_location_file(self, location_file_name: str):
#         """Open the location file as a DataFrame."""
#         file_extension = location_file_name.split('.')[-1]
#         if file_extension == 'csv':
#             self.location_df = pd.read_csv(location_file_name)
#         elif file_extension == 'parquet':
#             self.location_df = pd.read_parquet(location_file_name)
#         else:
#             raise ValueError("Unsupported file format: {}".format(file_extension))

#     def _save_location_dataframe(self):
#         """Save the updated location DataFrame to a new CSV file."""
#         dict_df = pd.DataFrame(self.add_location_geocoded).T.reset_index()
#         dict_df.columns = ['locationName', 'latitude', 'longitude']
#         combined_df = pd.concat([self.location_df, dict_df], ignore_index=True)
#         combined_df.to_csv('location_new.csv', index=False)

#     async def _geocode_address(self, location: str):
#         """Asynchronously geocode a single address."""
#         try:
#             print(location)
#             loop = asyncio.get_event_loop()
#             geocode_result = await loop.run_in_executor(None, self.app.geocode, location)
#             if geocode_result:
#                 # Extract latitude and longitude from the geocode result
#                 self.add_location_geocoded[location] = (geocode_result.latitude, geocode_result.longitude)
#                 self._save_location_dataframe()
#         except Exception as e:
#             print(f"Error geocoding location {location}: {e}")

#     async def _geocode_dataframe(self, df: pd.DataFrame):
#         """Asynchronously geocode all addresses in the DataFrame with missing lat/lon."""
#         unique_locations_missing = df[df['latitude'].isna()]['locationName'].unique()
#         tasks = [self._geocode_address(location) for location in unique_locations_missing]
#         await asyncio.gather(*tasks)  # Await the tasks to be processed

#     def geocode_dataframe(self, location_file_name: str, df: pd.DataFrame):
#         """Public method to geocode addresses and save results."""
#         self._open_location_file(location_file_name)
#         asyncio.run(self._geocode_dataframe(df))

# # Usage Example
# if __name__ == "__main__":
#     locator = GetCoordinatesNominatim()
#     locator.geocode_dataframe('location.csv', locator.location_df)
