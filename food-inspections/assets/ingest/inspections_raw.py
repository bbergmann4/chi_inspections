"""@bruin

name: ingest.inspections_raw
type: python
image: python:3.11
connection: GCP_ETL

materialization:
  type: table
  strategy: insert_delete
  unique_key: inspection_id

columns:
  - name: inspection_id
    type: integer
    primary_key: true
    description: Unique identifier for each inspection
  - name: dba_name
    type: string
    description: Doing Business As name of the establishment
  - name: aka_name
    type: string
    description: The alternate name of the establishment
  - name: license_number
    type: integer
    description: The license number of the establishment
  - name: facility_type
    type: string
    description: The type of facility
  - name: risk
    type: string
    description: The risk level of the establishment
  - name: address
    type: string
    description: The address of the establishment
  - name: city
    type: string
    description: The city where the establishment is located
  - name: state 
    type: string
    description: The state where the establishment is located
  - name: zip_code
    type: string
    description: The zip code where the establishment is located
  - name: inspection_date
    type: timestamp 
    description: The date when the inspection occurred
  - name: inspection_type
    type: string
    description: The type of inspection conducted
  - name: results
    type: string
    description: The results of the inspection
  - name: violations
    type: string
    description: The violations found during the inspection, multiple violations separated by pipe
  - name: latitude
    type: float
    description: The latitude of the establishment
  - name: longitude
    type: float
    description: The longitude of the establishment
  - name: location
    type: string
    description: The geocoded location of the establishment in "POINT (longitude latitude)" format 
  - name: extracted_at
    type: timestamp
    description: The timestamp when the data was extracted from the source system for lineage purposes
@bruin"""


import os
import pandas as pd
import requests
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth
from io import BytesIO
import time


def materialize():
    """
    Fetch Chicago Park District event data from the API endpoint.
    
    - Reads CHI_API environment variables
    - Fetches event data from the API in batches of 500 records until no more data is available or a safety offset limit is reached
    - Returns concatenated DataFrame with extracted_at timestamp
    """

    basic = HTTPBasicAuth(os.getenv('CHI_API_ID'), os.getenv('CHI_API_SECRET'))
    url = "https://data.cityofchicago.org/api/v3/views/qizy-d2wf/export.csv"
    names = ['inspection_id', 'dba_name', 'aka_name', 'license_number', 'facility_type', 'risk', 'address', 'city', 'state', 'zip_code', 'inspection_date', 'inspection_type', 'results', 'violations', 'latitude', 'longitude', 'location']
  
    max_retries = 5
    for attempt in range(max_retries):
        try:
          file = requests.post(url, auth=basic, timeout = 10)
          if file.status_code == 200:
            break
          if file.status_code != 200:
            raise Exception(f"Error fetching data: {file.status_code} - {file.text}")
        except Exception as e:
            print(f"Attempt {attempt + 1} failed with error: {e}")
            if attempt < max_retries - 1:
                print("Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                raise Exception("Max retries reached. Failed to fetch data.")

    df = pd.read_csv(BytesIO(file.content), names=names, header=0, parse_dates=['inspection_date'])
    if df.empty:
        raise Exception("Error fetching data:  datafile empty")
    
    df['extracted_at'] = datetime.utcnow().isoformat()

    print (f"Fetched {len(df)} records")
    # Add extracted_at timestamp for lineage
    return df
    


