import os
import json
import argparse
import asyncio
import aiohttp
import pandas as pd
import time
import datetime

# Asynchronous function to fetch job data from the API
async def fetch_jobs(session, url, query_params, auth):
    async with session.get(url, params=query_params, auth=auth) as response:
        return await response.json()  # Wait for the response and return it as JSON


# Asynchronous function to process each keyword and fetch jobs
async def process_keyword(session, keyword, url, key, csv_file_name, reed_jobs):
    query_params = {
        'keywords': keyword,
        'resultsToSkip': 0  # Initialize resultsToSkip for pagination
    }
    while True:
        try:
            response = await fetch_jobs(session, url, query_params, auth=aiohttp.BasicAuth(key, ''))
            # Append job results to the list
            reed_jobs.extend(response['results'])
            total_jobs_in_search = response['totalResults']
            # Save the results to a JSON file
            with open(csv_file_name, 'w') as f:
                json.dump(reed_jobs, f)
            
            print(f"{len(reed_jobs)} Job posts collected")
            print(f"{total_jobs_in_search} jobs available for '{keyword}'")
            print(f"Processing resultsToSkip={query_params['resultsToSkip']}")
            print(f"url: {url}")
            

            if int(total_jobs_in_search) - int(query_params['resultsToSkip']) < 0:
                break  # Break the loop if there are no more jobs to fetch
            else:
                # Increment resultsToSkip for the next batch
                query_params['resultsToSkip'] += 100
                
        except Exception as e:
            print(e)
            break  # Break the loop in case of an exception


# Asynchronous function to initiate job search for all keywords
async def jobs_search(keywords_list: list, csv_file_name: str):
    # Retrieve the API key from environment variables
    key = os.environ['REED_API_KEY']
    # Base URL for the Reed API
    url = "https://www.reed.co.uk/api/1.0/search?"
    # List to store job results
    reed_jobs = []

    async with aiohttp.ClientSession() as session:
        # Create tasks for each keyword and gather them asynchronously
        tasks = [asyncio.create_task(process_keyword(
            session, keyword, url, key, csv_file_name, reed_jobs)) for keyword in keywords_list]
        await asyncio.gather(*tasks)  # Wait for all tasks to complete

def save_file_to_parquet(file_name:str):
    df = pd.read_json(file_name).drop_duplicates(subset=['jobId']).reset_index(drop=True).pipe(lambda df: df.rename(
        columns={
            'jobId':'id',
            'employerName':'employer_name',
            'jobTitle':'job_title',
            'locationName':'location', 
            'expirationDate':'expiration_date',
            'date':'publication_date',
            'jobDescription':'description',
            'applications':'aplications',
            'jobUrl':'job_url'
        }
    ))
    df['salary']=df[['minimumSalary','maximumSalary']].mean(axis=1)
    df=df[['id',
            'job_title',
            'location',
            'salary',
            'job_url',
            'publication_date',
            'expiration_date',
            'description',
            'employer_name',
            'aplications',
           ]]
   
    parquet_name=file_name.split('.')[0]+".parquet"
    df.to_parquet(parquet_name,index=False)

    #Remove the .json file as it will not be used anymore
    for file_name in os.listdir():
        if file_name.endswith(".json"):
            os.remove(file_name)

# Main function to parse arguments and run the job search
def main():
    parser = argparse.ArgumentParser(
        description='Process keywords file and output file.')
    parser.add_argument('keywords_file', type=str,
                        help='Path to the keywords CSV file')
   
    args = parser.parse_args()
    keywords_list = pd.read_csv(args.keywords_file)['keywords'].values
    # Run the asynchronous job search
    start = time.perf_counter()

    output_file_name=f"reed_jobs_{datetime.date.today()}.json"
    asyncio.run(jobs_search(keywords_list=keywords_list,
                csv_file_name=output_file_name))
    end = time.perf_counter()
    script_duration=end-start
    print(f"Script duration: {script_duration/60:0.2f} minutes")
    save_file_to_parquet(output_file_name)
    

# Entry point of the script
if __name__ == '__main__':
    main()
