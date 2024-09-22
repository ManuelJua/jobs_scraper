import os
import requests
import json
import pandas as pd


def jobs_search(keywords_list: list, csv_file_name: str):
    # Set your API key and endpoint
    # Retrieve the API key from environment variables
    key = os.getenv('JOOBLE_API_KEY')
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
            # Send the POST request with the query parameters
            response = requests.post(url, json=query_params)

            # Check if the request was successful
            if response.status_code == 200:
                # Append the jobs from the response to the info_json list
                info_json.extend(response.json()['jobs'])
                if (len(response.json()['jobs'])) > 0:
                    with open(csv_file_name, 'w') as f:  # Open the jobs.json file in write mode
                        # Write the job information to the file
                        json.dump(info_json, f)
                    # Print a success message
                    print(
                        f"Script Executed at page {page} and data saved to 'jobs.json'")
                    page += 1  # Increment the page number for the next iteration
                else:
                    # Print a message if no more jobs are found
                    print(f"No more information available in page: {page}")
                    break  # Exit the loop if no more jobs are available
            else:
                # Print an error message if the request fails
                print(f"Error: {response.status_code} - {response.reason}")


def main():
    keywords_list = pd.read_csv('python_developer_keywords.csv')[
        'keywords'].values
    # Define the name of the output CSV file
    csv_file_name = "python_develoepr_reed_jobs.json"
    # Call the jobs_search function with the keywords list and CSV file name
    jobs_search(keywords_list=keywords_list, csv_file_name=csv_file_name)


if __name__ == '__main__':
    main()
