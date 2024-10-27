import os
import json
import argparse
import asyncio
import aiohttp
import pandas as pd

async def fetch_jobs(session, url, query_params, auth):
    async with session.get(url, params=query_params, auth=auth) as response:
        return await response.json()


async def process_keyword(session, keyword, url, key, csv_file_name, reed_jobs):
    query_params = {
        'keywords': keyword,
        'resultsToSkip': 0
    }
    while True:
        try:
            response = await fetch_jobs(session, url, query_params, auth=aiohttp.BasicAuth(key, ''))
            reed_jobs.extend(response['results'])

            total_jobs_in_search = response['totalResults']
            # Save the results to a JSON file
            with open(csv_file_name, 'w') as f:
                json.dump(reed_jobs, f)

            if int(total_jobs_in_search) - int(query_params['resultsToSkip']) < 0:
                break
            else:
                print(f"{len(reed_jobs)} Job posts collected")
                print(f"{total_jobs_in_search} jobs available for '{keyword}'")
                print(f"url: {url}")
                query_params['resultsToSkip'] += 100
        except Exception as e:
            print(e)
            break

async def jobs_search(keywords_list: list, csv_file_name: str):
    # Retrieve the API key from environment variables
    key = os.getenv('REED_API_KEY')
    # Base URL for the Reed API
    url = "https://www.reed.co.uk/api/1.0/search?"
    # List to store job results
    reed_jobs = []
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(process_keyword(session, keyword, url, key, csv_file_name, reed_jobs)) for keyword in keywords_list]
        await asyncio.gather(*tasks)

def main():
    parser = argparse.ArgumentParser(description='Process keywords file and output file.')
    parser.add_argument('keywords_file', type=str, help='Path to the keywords CSV file')
    parser.add_argument('output_file', type=str, help='Name of the output JSON file')
    args = parser.parse_args()
    keywords_list = pd.read_csv(args.keywords_file)['keywords'].values

    asyncio.run(jobs_search(keywords_list=keywords_list, csv_file_name=args.output_file))

if __name__ == '__main__':
    main()
