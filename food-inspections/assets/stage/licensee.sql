/* @bruin

name: stage.licensee
type: bq.sql
connection: GCP_ETL


materialization:
  type: table
  strategy: merge
  primary_key: licensee_id   
depends:
   - ingest.inspections_raw



# you can define column metadata and quality checks
columns: 
  - name: licensee_id 
    type: string
    primary_key: true
    description: a hashed identifier for the licensee, created by hashing the license number and dba name together
    checks: 
      - name: not_null
  - name: last_address_id
    type: string
    description: a hashed identifier for the address, created by hashing the address, city, state, and zip code together
  - name: license_number
    type: INT64
    description: the license number of the facility being inspected
  - name: dba_name
    type: string
    description: the "doing business as" name of the licensee
  - name: aka_names
    type: string
    description: a comma-separated list of all the "also known as" names associated with the licensee across all inspections
  - name: last_inspection_date
    type: date
    description: the date of the most recent inspection for the licensee
  - name: location_count
    type: INT64
    description: the number of unique locations (as defined by unique address_id) associated with the licensee across all inspections
  - name: facility_types
    type: string
    description: a comma-separated list of all the facility types associated with the licensee across all inspections
  - name: max_risk_category
    type: string
    description: the highest risk category assigned to the licensee across all inspections (e.g. "High Risk", "Medium Risk", "Low Risk")
  - name: last_risk_category
    type: string
    description: the risk category assigned to the licensee in their most recent inspection (e.g. "High Risk", "Medium Risk", "Low Risk")   



@bruin */

with licensee_inspections as (
  SELECT
  row_number() over (partition by license_number order by inspection_date desc) as rn,
    
    MD5(COALESCE(cast(license_number as string), '')||COALESCE(dba_name, '')) AS licensee_id,
    MD5(COALESCE(address, '')||COALESCE(city, 'CHICAGO')||COALESCE(state, 'IL')||COALESCE(zip_code, 0)) AS address_id,
    license_number,
    dba_name,
    aka_name,
    facility_type,
    risk as risk_category,
    inspection_date 
  FROM ingest.inspections_raw
)
, licensee_aggregated as (
  SELECT 
  licensee_id,
  count(distinct address_id) as location_count,
  string_agg(distinct aka_name, ', ') as aka_names,
  string_agg(distinct facility_type, ', ') as facility_types,
  max(risk_category) as max_risk_category
  FROM licensee_inspections
  GROUP BY licensee_id
  )
, licensee_latest as (
  SELECT  
  licensee_id,
  address_id as last_address_id,
  license_number,
  dba_name,
  risk_category as last_risk_category,
  inspection_date as last_inspection_date
  FROM licensee_inspections
  WHERE rn = 1
)

SELECT 
  ll.licensee_id,
  ll.last_address_id,
  ll.license_number,
  ll.dba_name,
  la.aka_names,
  ll.last_inspection_date,
  la.location_count,
  la.facility_types,
  la.max_risk_category,
  ll.last_risk_category
  FROM licensee_latest ll
  JOIN licensee_aggregated la ON ll.licensee_id = la.licensee_id