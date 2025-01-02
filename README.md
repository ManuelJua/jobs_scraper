# Job Search Script

This project allows you to search for job postings using the [Reed for developers](https://www.reed.co.uk/developers/jobseeker) API based on a list of keywords provided in a CSV file. 

The are to main scripts, reed_scraper.py, which get the job postings asynchronously using the API, and load_to_db.py which filter the new job posts and upload them to a postgres database deploey in NEON.

Besides, in the folder geocoding you will find a series of scripts design filter new locations, geocode the and upload them to a table in the database

Additionally, a [dashboard](https://jobsscraper-production.up.railway.app) created using Streamlit can be accessed at the following URL: https://jobsscraper-production.up.railway.app


## Requirements for main branch

- `pandas==2.0.3`
- `requests==2.32.3`
- `aiohttp==3.10.10`
- `SQLAlchemy==2.0.36`
- `psycopg2-binary==2.9.10`
- `numpy==1.24.4`
- `pyarrow==17.0.0`

## Requirements for streamlit_dashboard branch

- `pandas==2.0.3`
- `numpy==1.24.4`
- `plotly==5.24.1`
- `streamlit==1.39.0`
- `folium==0.17.0`
- `streamlit-folium==0.22.1`
- `SQLAlchemy==2.0.36`
- `psycopg2-binary==2.9.10`

## Setup

1. **Clone the repository** (if applicable):
    ```bash
    git clone https://github.com/ManuelJua/jobs_scraper.git 
    cd jobs_scraper
    ```

2. **Install the required libraries**:
    ```bash
    pip install -r requirements.txt
    ```


3. **Set up environment variables**:
    - `DATABASE_URL`: The connection string for your database.
    - `REED_API_KEY`: Your API key for accessing the Reed API.

## Usage

1. **Prepare your keywords CSV file**:
    - Create a CSV file with a column named `keywords` containing the search terms.

2. **Run the scraper**:
    ```bash
    python script.py <keywords_file>
    ```
    - `<keywords_file>`: Path to the CSV file containing the keywords.

3. **Load the data into the database**:
    ```sh
    python etl/load_to_db.py
    ```
