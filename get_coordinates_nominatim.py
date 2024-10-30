from geopy.geocoders import Nominatim
import pandas as pd


class getCoordinatesNominatim:
    def __init__(self):
        # Initialize the Nominatim geocoder with a user agent
        self.app = Nominatim(user_agent='jobs')
        self.add_location_geocoded = {}

    def _open_location_file(self):
        file_extension = self.location_file_name.split('.')[-1]
        if file_extension == 'csv':
            self.location_df = pd.read_csv(self.location_file_name)
        elif file_extension == 'parquet':
            self.location_df = pd.read_parquet(self.location_file_name)

    def _save_location_dataframe(self):
        dict_df = pd.DataFrame(self.add_location_geocoded).T.reset_index()
        dict_df.columns = ['locationName', 'latitude', 'longitude']
        combined_df = pd.concat([self.location_df, dict_df], ignore_index=True)
        combined_df.to_csv('location_new.csv', index=False)

    def _geocode_address(self, location):
        # Attempt to geocode the location
        try:
            geocode_result = self.app.geocode(location)
            print(geocode_result)
            if geocode_result:
                # Extract latitude and longitude from the geocode result
                self.add_location_geocoded[location] = (geocode_result.latitude,
                                                        geocode_result.longitude)
                self._save_location_dataframe()

        except Exception as e:
            # Handle any other exceptions and continue with the next location
            print(f"Error geocoding location {location}: {e}")

    def _geocode_dataframe(self, df):
        # Get unique location names from the DataFrame
        unique_locations_missing = df[df['latitude'].isna(
        )]['locationName'].unique()
        for location in unique_locations_missing:
            self._geocode_address(location)
        

    def geocode_dataframe(self, location_file_name, df):
        self.location_file_name=location_file_name
        self._geocode_dataframe(df)