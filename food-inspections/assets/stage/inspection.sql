/* @bruin

name: stage.inspection
type: bq.sql
connection: GCP_ETL


materialization:
  type: table
  partition_by: date_trunc(inspection_date, MONTH)
  strategy: merge
  primary_key: inspection_id   
depends:
   - ingest.inspections_raw



# you can define column metadata and quality checks
columns: 
  - name: inspection_id 
    type: INT64
    primary_key: true
    description: the unique identifier for each inspection
    checks: 
      - name: not_null 
  - name: license_number 
    type: INT64
    description: the license number of the facility being inspected
  - name: licensee_id 
    type: string 
    description: a hashed identifier for the licensee, created by hashing the license number and dba name together
  - name: address_id
    type: string
    description: a hashed identifier for the address, created by hashing the address, city, state, and zip code together
    checks:
      - name: not_null
  - name: inspection_date 
    type: date 
    description: the date of the inspection
    checks: 
      - name: not_null
      - name: date_in_past
      - name: date_format
      - format: "%Y-%m-%d"
      - name: date_not_future
      - name: date_range
      - start_date: "2018-01-01"
  - name: inspection_type 
    type: string 
    description: the type of inspection conducted (e.g. "Canvass", "Complaint", "License Renewal", "Pre-permit")
    checks: 
      - name: accepted_values
        value: ['LICENSE','COMPLAINT','SHORT FORM COMPLAINT','CANVASS','LICENSE RE-INSPECTION','CANVASS RE-INSPECTION','COMPLAINT RE-INSPECTION','SUSPECTED FOOD POISONING','NON-INSPECTION','RECENT INSPECTION','CONSULTATION','NOT READY','SUSPECTED FOOD POISONING RE-INSPECTION','ASSESSMENT','SPECIAL EVENTS (FESTIVALS)','COVID COMPLAINT','NO ENTRY','OUT OF BUSINESS']
  - name: results 
    type: string 
    description: the results of the inspection (e.g. "Pass", "Fail", "Pass w/ Conditions")
    checks: 
      - name: accepted_values
        value: ["Pass", "Fail", "Pass w/ Conditions", "Not Ready", "No Entry", "Out of Business", "Business Not Located"]
  - name: violation_count  
    type: int 
    description: the number of violations found during the inspection
  - name: violation_details
    type: string
    description: details about the violations found during the inspection
  - name: completed_flag
    type: boolean
    description: a flag indicating whether the inspection is completed, derived from the results column
    checks:
      - name: not_null
      - name: accepted_values
        value: [true, false]
  - name: pass_flag
    type: boolean
    description: a flag indicating whether the inspection passed or failed, derived from the results column
    checks:
      - name: not_null
      - name: accepted_values
        value: [true, false]

    
# you can also define custom checks 
custom_checks:
  - name: row count is greater than zero 
    description: this check ensures that the table is not empty 
    query: SELECT count(*) > 1 FROM ingest.inspections_raw
    value: 1




@bruin */

SELECT 
    inspection_id ,
    coalesce(license_number, 0) as license_number,
    MD5(COALESCE(cast(license_number as string), '')||COALESCE(dba_name, '')) AS licensee_id,
    MD5(COALESCE(address, '')||COALESCE(city, 'CHICAGO')||COALESCE(state, 'IL')||COALESCE(zip_code, 0)) AS address_id,
    inspection_date,
    upper(inspection_type) as inspection_type,
    results,
    -- count the number of violations by splitting the violations string on the delimiter and counting the resulting elements
    CASE 
        WHEN violations IS NULL THEN 0 
        ELSE array_length(REGEXP_EXTRACT_ALL(violations, 'Comments'))
        -- Generally, each violation is described with a rule citation followed by a description headed with the word Comments, so we can count the number of violations by counting the number of comments in the violations string
    END AS violation_count,
    violations AS violation_details,
    case 
        when results IN ('Pass', 'Pass w/ Conditions', 'Fail') then true
        else false
    end as completed_flag,
    case 
        when results IN ('Pass', 'Pass w/ Conditions') then true
        else false
    end as pass_flag
FROM ingest.inspections_raw