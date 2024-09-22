import os
import requests
import json
def jobs_search(keywords_list: list, csv_file_name: str):
    # Set your API key and endpoint
    key = os.getenv('JOOBLE_API_KEY')  # Retrieve the API key from environment variables
    url = f'https://jooble.org/api/{key}'  # Construct the API endpoint URL

    for keyword in keywords_list:
        page = 1  # Initialize the page number
        info_json = []  # Initialize an empty list to store job information
        while True:
            # Define your query parameters
            query_params = {
                'keywords': keyword,  # Set the keyword for the job search
                'location': '',  # Location is left empty for a global search
                'page': f'{page}'  # Set the current page number
            }

            # Make the POST request using requests
            response = requests.post(url, json=query_params)  # Send the POST request with the query parameters

            # Check if the request was successful
            if response.status_code == 200:
                info_json.extend(response.json()['jobs'])  # Append the jobs from the response to the info_json list
                if (len(response.json()['jobs'])) > 0:
                    with open(f"jobs.json", 'w') as f:  # Open the jobs.json file in write mode
                        json.dump(info_json, f)  # Write the job information to the file
                    print(f"Script Executed at page {page} and data saved to 'jobs.json'")  # Print a success message
                    page += 1  # Increment the page number for the next iteration
                else:
                    print(f"No more information available in page: {page}")  # Print a message if no more jobs are found
                    break  # Exit the loop if no more jobs are available
            else:
                print(f"Error: {response.status_code} - {response.reason}")  # Print an error message if the request fails
