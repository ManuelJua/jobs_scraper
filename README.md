# Job Search Script

This script allows you to search for job postings using the Reed API based on a list of keywords provided in a CSV file. The results are saved in a JSON file.

## Requirements

- Python 3.x
- `pandas==2.0.3` library
- `requests==2.32.3` library

## Setup

1. **Clone the repository** (if applicable):
    ```bash
    git clone https://github.com/ManuelJua/jobs_scraper.git 
    cd jobs_scraper
    ```

2. **Install the required libraries**:
    ```bash
    pip install requests pandas
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

## Additional Analysis

 - There is a file called reed_analysis.ipynb that analyzes the data generated by the scraper.  