# chi_inspections
Data Engineering Zoomcamp 2026 Project

# Quickstart

Use this repo to build your own data pipeline for Chicago Food Safety Inspections from the Chicago Data Portal to the Google Cloud.
1.  Create a [Data Portal API Key](https://data.cityofchicago.org/signup) and a [GCP Service Account](https://developers.google.com/workspace/guides/create-credentials) and load it in to [Github Codespace Secrets](https://docs.github.com/en/codespaces/managing-your-codespaces/managing-your-account-specific-secrets-for-github-codespaces) (CHI_API_ID, CHI_API_SECRET, GCP_PROJECT_ID, GCP_SERVICE_CRED)
2.  Open in Github Codespace and run `bruin run food-inspections --full-refresh`
3.  Copy [my report](https://datastudio.google.com/s/gOLV-FVKGrk) and link your own data.
4.  When you are done, delete your assets in bigquery with the cleanup script:  `uv run cleanup.py`

Full instructions below.

# The Problem
This workflow aims to build a pipeline from the City of Chicago’s Data Portal to a Google BigQuery data warehouse and dashboard. The pipeline will load, validate, and re-structure this data.


This pipeline will answer questions like:
 -  How do inspection results compare month to month or year to year?  In other words, what is the worst month to eat out in Chicago?
 -  Which restaurants get the most violations?  What places should I avoid?


## Project Goals
- Create an end-to-end ELT process ending with a simple report
- Make it replicable for peer review with easy-to-follow documentation, minimal configuration, and <15 minute workflow
- Explore a Chicago Data Portal dataset to learn how it works and create a data model for future exploration

## Tools & Approach
In order to accomplish these goals, I plan to use the following tools:
- Github codespaces as an easy replicable virtual environment, 
- Create Python environment in uv for replicable package management,
- Orchestrate resource allocation, ETL, and testing with Bruin,
- Use Python and SQL assets within Bruin to ingest and reshape data,
- Utilize Google BigQuery and Data Studio for storage, analysis, and reporting,
- Provide a Python script to remove resources when done.  

# The Data

## Chicago Data Portal

The City of Chicago maintains a large library of public datasets in a variety of formats.  Many of these are accessible through their SODA3 API.  The data from this project is sourced from the following *Food Inspections Since 07/01/2018* dataset.

This metadata page provides a description of the dataset, columns, datatypes, and the api url.

[Food Inspections Metadata](https://data.cityofchicago.org/Health-Human-Services/Food-Inspections-7-1-2018-Present/qizy-d2wf/about_data)

More information on understanding the data is available here:  

[Food Inspection Data Description](docs/foodinspections_description.pdf)

## The Data Model

Our end result of this process is a pretty simple star schema:
  - Inspection:  Our fact table with 1 row per inspection
  - Licensee:  A dimension table describing the business, facility, or kitchen
  - Location:  A dimension table describing the address 

![Data Model](docs/images/data_model_squail.png)

# Recreating this Data Pipeline

## Initial Setup

Feel free to create a fork of this repository or use github importer (plus + mark in top right) to make a disconnected copy in your github.  

This project is intended to be run in Github Codespaces for quick replicability.  All of the tools needed will be installed using the .devcontainer.json file and you will provide all credentials needed through codespace secrets.  It is possible to run this process in a docker image with small changes:  Use [docs/dockeroption.yml](docs/dockeroption.yml) as your starting point.    

Before getting started, you will need to have your own Google Cloud Platform account with billing or a trial enabled. The storage and compute resources requred are relatively trivial.  You will also be required to provide basic contact information to sign up for a free API account with the City of Chicago (described below).

## Credentials

### Google Cloud Platform

You will need to create service account credentials [(full instructions here)](https://developers.google.com/workspace/guides/create-credentials).  

1. Go to Menu > IAM & Admin > Service Accounts
2. Click Create service account
3. Fill out details and assign roles to the account
4. Once completed, you will return to your list of service accounts.  Click Keys > Add Key > Create New Key > JSON
5. Download that key to a safe location. 

You will need to provide the GCP Service Account the following minimum privileges:
 - roles/bigquery.dataEditor 
 - roles/bigquery.jobUser 
 - roles/bigquery.dataViewer
   
Or, you can simply assign the BigQuery Admin role.  

### Chicago Data Portal API

In order to call the API, you will need to have an api key. You can do this by visiting the [developer page](https://data.cityofchicago.org/profile/edit/developer_settings). 

1. Click create new account at the bottom and fill out your account information 
2. When requested, verify your email address and [sign in](https://data.cityofchicago.org/login )
3. Now, your username will appear on the top right.  Click on your username drop down and go to [Developer Settings](https://data.cityofchicago.org/profile/edit/developer_settings)
4.  Now you should see a button to create a new API key.  Copy these secrets in a safe place.

### Storing Credentials

In order to safely store secrets in this project, I am using codespace secrets that act as environment variables within the VM.
1.  Login to github in your browswer, open your copy of this repo, and go to settings in the upper right 
2.  Under  Security there is "Secrets and variables" header.  Click on Codespaces.

<img src="https://github.com/bbergmann4/chi_inspections/blob/4f0e0a10b6f949fa0ebbc1885975b86e904b3b5c/docs/images/github_codespace_secrets.png" width="30%">

3.  Create the following 4 codespace secrets in this repo:

 **Your GCP credentials**

Service Account
  - Name:  GCP_SERVICE_CRED
  - Secret:  _The contents of your ETL service account json_

Project ID
  - Name:   GCP_PROJECT_ID
  - Secret:  _The project id associated with the above service account (looks like random-word-1234-a2)_

**Your Chicago Data Portal credenials**
 
  - Name:  CHI_API_ID
  - Secret:  _The API Key ID provided by data.cityofchicago.org_
   
  - Name:  CHI_API_SECRET
  - Secret:  _The API Key Secret provided by data.cityofchicago.org_

  ## The Pipeline
  ### Bruin
  This project uses Bruin to orchestrate the following tasks in our ELT pipeline:
   - Resource Allocation:  Bruin will create three datasets in BigQuery for our tables:  ingest, stage, report.
   - Extraction:  Using a python script, Bruin will run the an API call to the Chicago Data Portal and download a csv of the raw inspection data.
   - Load:  Bruin will create a bigquery table and load our raw data.   
   - Transformations:  We will shape that data in Big Query into three staging tables and two reporting tables.
   - Quality Checks:  Bruin will run 20 quality checks to help us identify any issues.  

  ### Running the Pipeline

  Once you have copied this repo and loaded the credentials, you should have everything you need to run the pipeline:
  1.  In your repo in github, click the green "Code" button and click "create a codespace on main".

<img src="https://github.com/bbergmann4/chi_inspections/blob/690a854990a1d6ddb227952db04752e7b748a756/docs/images/create_codespace.png" width="30%">

  The first run may take longer as it build the devcontainer and installs dependencies.


  2.  The developer environment will open.  If you like, you can run in browser or [switch to an IDE like VSCODE](https://docs.github.com/en/codespaces/developing-in-a-codespace/using-github-codespaces-in-visual-studio-code)

  3.  In the terminal, run 

  ```
  bruin run food-inpections --full-refresh

  ```

  On the first run it will take longer as bruin uses uv to load dependencies. 

To learn more about this process, go to [food-inspections/README.MD](food-inspections/README.md)

## The Report

I am using Google Data Studio to show results.

You can see my report [here](https://datastudio.google.com/s/gOLV-FVKGrk) or see a static pdf copy in [docs/report_2026_sofar.pdf](docs/report_2026_sofar.pdf).  

The report uses the report.inspections_by_month and report.inspections_by_licensee tables created in the pipeline.

Three charts show monthly data from ispections_by_month, for example this show inspection trends for 2025:
![Monthly Inspection Trends 2025](docs/images/Inspections_by_Month_Time_series_2025.png)
On the live report, choose the year(s) you want to evaluate using the year dropdown at the bottom of the clipboard on the top left.

And a graphic on the left provides the "Worst Offender" of the last six months from the inspections_by_licensee table.
<img src="https://github.com/bbergmann4/chi_inspections/blob/8d89d0414f8ee0ea6a55020f13bcff0375f3b759/docs/images/licensee_worstviolations_last6mo.png" width="30%">


You can change the period at the bottom of this clipboard.

## Recreating the Report

Once you have run the pipeline, recreating the report is simple.  

1.  Go to my report online [here](https://datastudio.google.com/s/gOLV-FVKGrk)
2.  Click the dots to make a copy 
<img src="https://github.com/bbergmann4/chi_inspections/blob/bb4dbedef28c1fd6ce11725c9317cba2f4244b3d/docs/images/make_a_copy.png" width="30%">
It may ask you some setup steps if you haven't used Data Studio before.

3.  Now, it will give you options to replace my datasource with your own.  You can add the tables from your pipeline by clicking on the dropdown and selecting add new datasource.  
<img src="https://github.com/bbergmann4/chi_inspections/blob/bb4dbedef28c1fd6ce11725c9317cba2f4244b3d/docs/images/copy_datasource.png" width="30%">
Click on bigquery and select your project and the "report" schema created by the pipeline.  There you should have copies of each table used for this report.    

## Cleanup

If you no longer wish to use this pipeline or dataset, you can run the cleanup script.

WARNING:  This will permanently delete the datasets you created in BigQuery

```
uv run cleanup.py
```

If you are planning to try to rerun this pipeline from scratch, you may want to run `bruin clean` to clear your cache/log history or run `bruin run food-inspections --full-refresh` when rebuilding.
