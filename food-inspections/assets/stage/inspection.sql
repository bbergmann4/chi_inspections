/* @bruin

name: stage.inspection
type: bigquery
materialization:
  type: table
   
depends:
   - ingest.inspections_raw


# you can define column metadata and quality checks
columns: 
  - name: inpection_id 
    type: integer
    primary_key: true
    description: the unique identifier for each inspection
    checks: 
      - name: not_null 
      - name: unique
  - name: license_number 
    type: integer 
    description: the license number of the facility being inspected
    checks: 
      - name: not_null  
  - name: licensee_id 
    type: string 
    description: a hashed identifier for the licensee, created by hashing the license number and dba name together
    checks: 
      - name: not_null
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
      - accepted_values: ['COMPLAINT','SHORT FORM COMPLAINT','CANVASS','LICENSE RE-INSPECTION','CANVASS RE-INSPECTION','COMPLAINT RE-INSPECTION','SUSPECTED FOOD POISONING','NON-INSPECTION','RECENT INSPECTION','CONSULTATION','NOT READY','SUSPECTED FOOD POISONING RE-INSPECTION','ASSESSMENT','SPECIAL EVENTS (FESTIVALS)','COVID COMPLAINT','NO ENTRY','OUT OF BUSINESSCOMPLAINT','SHORT FORM COMPLAINT','CANVASS','LICENSE RE-INSPECTION','CANVASS RE-INSPECTION','COMPLAINT RE-INSPECTION','SUSPECTED FOOD POISONING','NON-INSPECTION','RECENT INSPECTION','CONSULTATION','NOT READY','SUSPECTED FOOD POISONING RE-INSPECTION','ASSESSMENT','SPECIAL EVENTS (FESTIVALS)','COVID COMPLAINT','NO ENTRY','OUT OF BUSINESS]
  - name: results 
    type: string 
    description: the results of the inspection (e.g. "Pass", "Fail", "Pass w/ Conditions")
    checks: 
      - name: accepted_values
      - accepted_values: ["Pass", "Fail", "Pass w/ Conditions", "Not Ready", "No Entry", "Out of Business", "Business Not Located"]
  - name: violation_count  
    type: int 
    description: the number of violations found during the inspection
  - name: violation_details
    type: string
    description: details about the violations found during the inspection
    
# you can also define custom checks 
custom_checks:
  - name: row count is greater than zero 
    description: this check ensures that the table is not empty 
    query: SELECT count(*) > 1 FROM dataset.player_stats
    value: 1




@bruin */

SELECT 
    inspection_id,
    license_number,
    MD5(license_number::text||dba_name) AS licensee_id,
    MD5(address||nvl(city, 'CHICAGO')||state||zip_code) AS address_id,
    inspection_date,
    upper(inspection_type) as inspection_type,
    results,
    -- count the number of violations by splitting the violations string on the delimiter and counting the resulting elements
    CASE 
        WHEN violations IS NULL THEN 0 
        ELSE substring(violations, 1, position(',' in violations) - 1)::int
    END AS violation_count,
    violations AS violation_details