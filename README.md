# Job Search Script

This script allows you to search for job postings using the [Reed for developers](https://www.reed.co.uk/developers/jobseeker) API based on a list of keywords provided in a CSV file. The script works asynchronously, and the results are saved in a JSON file.

Additionally, a [dashboard](https://streamlit-dashboard-2sq9.onrender.com/) created using Streamlit can be accessed at the following URL: https://streamlit-dashboard-2sq9.onrender.com/

A more comprehensive data analysis can be found in `reed_analys.ipynb`.

The next goals of this project are to automate the job extraction and geocoding pipeline and update the dashboard automatically with the latest available jobs.

## Requirements for main branch

- `pandas==2.0.3`
- `requests==2.32.3`
- `aiohttp==3.10.10`

## Requirements for streamlit_dashboard branch

- `pandas==2.0.3`
- `numpy==1.24.4`
- `plotly==5.24.1`
- `streamlit==1.39.0`
- `folium==0.17.0`
- `streamlit-folium==0.22.1`

## Requirements for dash_deploy branch

- `pandas==2.0.3`
- `numpy==1.24.4`
- `plotly==5.24.1`
- `dash==2.18.1`

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

3. **Set up your Reed API key**:
    - Obtain an API key from Reed API.
    - Set the API key as an environment variable:
        ```bash
        export REED_API_KEY='your_api_key'
        ```

## Usage

1. **Prepare your keywords CSV file**:
    - Create a CSV file with a column named `keywords` containing the search terms.

2. **Run the script**:
    ```bash
    python script.py <keywords_file> <output_file>
    ```
    - `<keywords_file>`: Path to the CSV file containing the keywords.
    - `<output_file>`: Name of the output JSON file where the job results will be saved.

## Example

```bash
python reed_scraper.py keywords.csv jobs.json
```