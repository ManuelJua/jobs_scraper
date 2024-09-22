import os
import json

import requests
import pandas as pd


def jobs_search(keywords_list: list, csv_file_name: str):
    # Retrieve the API key from environment variables
    key = os.getenv('REED_API_KEY')
    # Base URL for the Reed API
    url = "https://www.reed.co.uk/api/1.0/search?"

    # List to store job results
    reed_jobs = []
    for keyword in keywords_list:
        # Define query parameters for the API request
        query_params = {
            'keywords': keyword,  # Search keywords
            # 'locationName':'United Kingdom',  # Location of the job
            # 'distanceFromLocation':'20',  # Distance from location in miles (default is 10)
            # 'resultsToTake': 100, # Maximum number of results to return (default and limited to 100 results)
            'resultsToSkip': 0, # Number of results to skip (used with resultsToTake for paging)
            # 'employerId':'',  # ID of employer posting job
            # 'employerProfileId':'',  # Profile ID of employer posting job
            # 'permanent':'',  # True/false for permanent jobs
            # 'contract':'',  # True/false for contract jobs
            # 'temp':'',  # True/false for temporary jobs
            # 'partTime':'',  # True/false for part-time jobs
            # 'fullTime':'',  # True/false for full-time jobs
            # 'minimumSalary':'',  # Lowest possible salary e.g. 20000
            # 'maximumSalary':'',  # Highest possible salary e.g. 30000
            # 'postedByRecruitmentAgency':'',  # True/false for jobs posted by recruitment agencies
            # 'postedByDirectEmployer':'',  # True/false for jobs posted by direct employers
            # 'graduate':'',  # True/false for graduate jobs
        }
        while True:
            # Make the API request
            response = requests.get(url, params=query_params, auth=(key, ''))
            # Extend the job results list with the new results
            reed_jobs.extend(response.json()['results'])
            # Get the total number of jobs in the search
            total_jobs_in_search = response.json()['totalResults']
            # Save the results to a JSON file
            with open( csv_file_name, 'w') as f:
                json.dump(reed_jobs, f)

            # Check if there are more jobs to fetch
            if int(total_jobs_in_search) - int(query_params['resultsToSkip']) < 0:
                break
            else:
                # Print the number of job posts collected and available
                print(f"{len(reed_jobs)} Job posts collected")
                print(f"{total_jobs_in_search} jobs available for '{keyword}'")
                print(f"url: {response.url}")

                # Update the resultsToSkip parameter for the next batch of results
                query_params['resultsToSkip'] = int(query_params['resultsToSkip']) + 100


def main():
    # Read the list of keywords from a CSV file
    keywords_list = pd.read_csv('python_developer_keywords.csv')['keywords'].values
    # Define the name of the output CSV file
    csv_file_name = "python_developer_reed_jobs.json"
    # Call the jobs_search function with the keywords list and CSV file name
    jobs_search(keywords_list=keywords_list, csv_file_name=csv_file_name)


if __name__ == '__main__':
    main()
