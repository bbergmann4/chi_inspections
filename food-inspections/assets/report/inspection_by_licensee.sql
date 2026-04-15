/* @bruin

name: report.inspection_by_licensee
type: bq.sql
connection: GCP_ETL


materialization:
  type: table
  strategy: truncate+insert
depends:
   - stage.inspection



# you can define column metadata and quality checks
columns: 
  - name: metric
    type: string
    description: whether the licensee had the most violations or inspections in the given period
    checks:
      - name: not_null
  - name: period
    type: string
    description: the period for which the licensee had the most violations/inspections
    checks:
      - name: not_null
  - name: dba_name
    type: string
    description: the dba name associated with the licensee
  - name: num_violations
    type: integer
    description: the number of violations for the licensee in the given period
  - name: num_inspections
    type: integer
    description: the number of inspections for the licensee in the given period   
   
@bruin */
with licensee_inspections as (
  select
  licensee_id,
  dba_name,
  SUM(CASE WHEN 
  cast(inspection_date as date)  >= date_trunc(current_date(), MONTH) and cast(inspection_date as date)  < date_trunc( date_add(current_date(), interval 1 MONTH), MONTH)
  THEN violation_count
  ELSE 0 END ) as violations_this_month,
  SUM(CASE WHEN 
  cast(inspection_date as date)  >= date_trunc(date_add(current_date(), interval -1 MONTH), MONTH) and cast(inspection_date as date)  < date_trunc(current_date(), MONTH)
  THEN violation_count 
  ELSE 0 END ) as violations_last_month,
  SUM(CASE WHEN 
  cast(inspection_date as date)  >= date_trunc(date_add(current_date(), interval -6 MONTH), MONTH) and cast(inspection_date as date)  < date_trunc(date_add(current_date(), interval 1 MONTH), MONTH)
  THEN violation_count
  ELSE 0 END ) as violations_last_6_months,
  SUM(CASE WHEN 
  cast(inspection_date as date)  >= date_trunc(date_add(current_date(), interval -12 MONTH), MONTH) and cast(inspection_date as date)  < date_trunc(date_add(current_date(), interval 1 MONTH), MONTH)
  THEN violation_count
  ELSE 0 END ) as violations_last_12_months,
  SUM(CASE WHEN 
  cast(inspection_date as date)  >= date_trunc(current_date(), MONTH) and cast(inspection_date as date)  < date_trunc( date_add(current_date(), interval 1 MONTH), MONTH)
  THEN 1
  ELSE 0 END ) as inspections_this_month,
  SUM(CASE WHEN 
  cast(inspection_date as date)  >= date_trunc(date_add(current_date(), interval -1 MONTH), MONTH) and cast(inspection_date as date)  < date_trunc(current_date(), MONTH)
  THEN 1
  ELSE 0 END ) as inspections_last_month,
  SUM(CASE WHEN 
  cast(inspection_date as date)  >= date_trunc(date_add(current_date(), interval -6 MONTH), MONTH) and cast(inspection_date as date)  < date_trunc(date_add(current_date(), interval 1 MONTH), MONTH)
  THEN 1
  ELSE 0 END ) as inspections_last_6_months,
  SUM(CASE WHEN 
  cast(inspection_date as date) >= date_trunc(date_add(current_date(), interval -12 MONTH), MONTH) and cast(inspection_date as date) < date_trunc(date_add(current_date(), interval 1 MONTH), MONTH)
  THEN 1
  ELSE 0 END ) as inspections_last_12_months
  from stage.inspection
  left join stage.licensee using (licensee_id)
  group by 1,2
)

, ranked as (
  select
  licensee_id,
  dba_name,
  violations_this_month,
  violations_last_month,
  violations_last_6_months,
  violations_last_12_months,
  inspections_this_month,
  inspections_last_month,
  inspections_last_6_months,
  inspections_last_12_months,
  row_number() over (order by violations_this_month desc) as rank_violations_this_month,
  row_number() over (order by violations_last_month desc) as rank_violations_last_month,
  row_number() over (order by violations_last_6_months desc) as rank_violations_last_6_months,
  row_number() over (order by violations_last_12_months desc) as rank_violations_last_12_months,
  row_number() over (order by inspections_this_month desc) as rank_inspections_this_month,
  row_number() over (order by inspections_last_month desc) as rank_inspections_last_month,
  row_number() over (order by inspections_last_6_months desc) as rank_inspections_last_6_months,
  row_number() over (order by inspections_last_12_months desc) as rank_inspections_last_12_months
  from licensee_inspections
)

SELECT
'Violations' as metric,
'This Month' as period,
dba_name,
violations_this_month as num_violations,
inspections_this_month as num_inspections
FROM ranked
WHERE rank_violations_this_month = 1
UNION ALL
SELECT
'Violations' as metric,
'Last Month' as period,
dba_name,
violations_last_month as num_violations,
inspections_last_month as num_inspections
FROM ranked
WHERE rank_violations_last_month = 1
UNION ALL
SELECT
'Violations' as metric,
'Last 6 Months' as period,
dba_name,
violations_last_6_months as num_violations,
inspections_last_6_months as num_inspections
FROM ranked
WHERE rank_violations_last_6_months = 1
UNION ALL
SELECT
'Violations' as metric,
'Last 12 Months' as period,
dba_name,
violations_last_12_months as num_violations,
inspections_last_12_months as num_inspections
FROM ranked
WHERE rank_violations_last_12_months = 1
UNION ALL
SELECT
'Inspections' as metric,
'This Month' as period,
dba_name,
violations_this_month as num_violations,
inspections_this_month as num_inspections
FROM ranked
WHERE rank_inspections_this_month = 1
UNION ALL
SELECT
'Inspections' as metric,
'Last Month' as period,
dba_name,
violations_last_month as num_violations,
inspections_last_month as num_inspections
FROM ranked
WHERE rank_inspections_last_month = 1
UNION ALL
SELECT
'Inspections' as metric,
'Last 6 Months' as period,
dba_name,
violations_last_6_months as num_violations,
inspections_last_6_months as num_inspections
FROM ranked
WHERE rank_inspections_last_6_months = 1
UNION ALL
SELECT
'Inspections' as metric,
'Last 12 Months' as period,
dba_name,
violations_last_12_months as num_violations,
inspections_last_12_months as num_inspections
FROM ranked
WHERE rank_inspections_last_12_months = 1