/* @bruin

name: stage.location
type: bq.sql
connection: GCP_ETL


materialization:
  type: table
  strategy: merge
  primary_key: address_id   
depends:
   - ingest.inspections_raw



# you can define column metadata and quality checks
columns: 
  - name: address_id 
    type: string
    primary_key: true
    description: a hashed identifier for the licensee, created by hashing the license number and dba name together
    checks: 
      - name: not_null
  - name: facility_types
    type: string
    description: a comma-separated list of all the facility types associated with the location across all inspections
  - name: full_address
    type: string
    description: the full address of the location, created by concatenating the address, city, state, and zip code together
  - name: address
    type: string
    description: the street address of the location
  - name: city
    type: string
    description: the city of the location
  - name: state
    type: string
    description: the state of the location
    checks:
      - name: string_length
        max_length: 2
  - name: zip_code
    type: string
    description: the zip code of the location
    checks:
      - name: string_length
        max_length: 5
  - name: latitude
    type: float
    description: the latitude of the location
  - name: longitude
    type: float
    description: the longitude of the location

@bruin */

with location_inspections as (
  SELECT
    row_number() over (partition by MD5(COALESCE(address, '')||COALESCE(city, 'CHICAGO')||COALESCE(state, 'IL')||COALESCE(zip_code, 0)) order by inspection_date desc) as rn,
    MD5(COALESCE(address, '')||COALESCE(city, 'CHICAGO')||COALESCE(state, 'IL')||COALESCE(zip_code, 0)) AS address_id,
    facility_type,
    COALESCE(address, '')||COALESCE(city, 'CHICAGO')||COALESCE(state, 'IL')||COALESCE(cast(zip_code as string), '') as full_address,
    address,
    coalesce(city, 'CHICAGO') as city,
    coalesce(state, 'IL') as state,
    coalesce(cast(zip_code as string),'00000') as zip_code,
    latitude,
    longitude
  FROM ingest.inspections_raw
)
, location_aggregates as (
  SELECT
    address_id,
    string_agg(distinct facility_type, ', ') as facility_types
  FROM location_inspections
  group by address_id
)

SELECT 
  loc.address_id,
  facility_types,
  full_address,
  address,
  city,
  state,
  zip_code,
  latitude,
  longitude
FROM location_inspections loc
JOIN location_aggregates USING (address_id)
WHERE rn = 1