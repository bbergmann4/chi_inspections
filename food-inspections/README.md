## How to Run this Pipline
From the root directory (CHI_INSPECTIONS)

```run bruin food-inspections```


## What's happening

The pipeline is orchestrating the following process
- Initializing a virtual environment and installing dependencies
- Connecting to your GCP Project using the instructions in .bruin.yaml to pull from the github secrets you provided 
- Validating the scripts in the pipeline
- Extraction:  Running assets/ingest/inspections_raw.py to
    - Retrieve your API secrets and request the data from the Chicago Data Portal
    - Download the data into a dataframe
    - Materialize a table in BigQuery
- Transformation:  Running assets/stage/inspections.sql
    - Creates a normalized inspections table and fills in null values
    - Creates unique identifiers for the licensees/businesses and their locations/addresses.
    - Runs the vast majority of checks
    - Loads in to bigquery table stage.inspection
- Tranformation:  Running assets/stage/licensee.sql and location.sql
    - Using the identifiers created in inspections to create unique rows by licensee and location
    - Loads in to stage.licensee and stage.location respectively
- Reporting:  Runnign assets/report/inspection_by_month.sql and inspection_by_licensee.sql:
    - Rolls up aggregates of inspection by year and month for a month-by-month report
    - Ranks licensees by most inspections or violations within certain periods (this month, last month, last six months, last 12 months)
    - Loads in to report.inspection_by_month with unique rows for year/month and report.inspection_by_licensee with unique rows by metric and period.
- Flags all the checks passed and failed 
