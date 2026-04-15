/* @bruin

name: report.inspection_by_month
type: bq.sql
connection: GCP_ETL


materialization:
  type: table
  strategy: truncate+insert
depends:
   - stage.inspection



# you can define column metadata and quality checks
columns: 
  - name: year
    type: string
    description: the year of the inspection
    checks: 
      - name: accepted_values
        value: ['2018','2019','2020','2021','2022','2023','2024', '2025', '2026']
  - name: month
    type: string
    description: the month of the inspection (in numeric format)
  - name: full_month
    type: string
    description: the month of the inspection (in full name format)

  - name: num_inspections
    type: int64
    description: the total number of inspections conducted in the month
    checks:
      - name: greater_than
        value: 0
  - name: num_locations_visited
    type: int64
    description: the total number of unique locations visited in the month 
    checks:
      - name: greater_than
        value: 0    
  - name: num_businesses_visited
    type: int64
    description: the total number of unique businesses visited in the month
    checks:
      - name: greater_than
        value: 0
  - name: num_passed
    type: int64
    description: the total number of inspections that passed in the month
    checks:
      - name: non_negative
  - name: num_completed
    type: int64
    description: the total number of inspections that were completed (i.e. had a result of "Pass", "Fail", or "Pass w/ Conditions") in the month
    checks:
      - name: non_negative
  - name: num_failed
    type: int64
    description: the total number of inspections that failed in the month
    checks:
      - name: non_negative
  - name: pass_rate
    type: float64
    description: the percentage of completed inspections that passed in the month
    checks:
      - name: between
        min_value: 0.0
        max_value: 1.0
  - name: total_violations
    type: int64
    description: the total number of violations recorded in the month
    checks:
      - name: non_negative
  - name: avg_violations_per_inspection
    type: float64
    description: the average number of violations per inspection in the month
    checks:
      - name: non_negative
  - name: max_violations_per_inspection
    type: int64
    description: the maximum number of violations recorded in a single inspection in the month
  - name: avg_violations_per_pass
    type: float64
    description: the average number of violations per passed inspection in the month
  - name: avg_violations_per_fail
    type: float64
    description: the average number of violations per failed inspection in the month
  - name: avg_violations_per_completed
    type: float64
    description: the average number of violations per completed inspection in the month

@bruin */

select
format_date( '%Y',cast(inspection_date as date)) as year,
format_date('%m', cast(inspection_date as date) ) as month,
format_date('%B', cast(inspection_date as date) ) as full_month,
count(1) as num_inspections,
count(distinct address_id) as num_locations_visited,
count(distinct licensee_id) as num_businesses_visited,
countif(pass_flag) as num_passed,
countif(completed_flag) as num_completed,
countif(completed_flag) - countif(pass_flag) as num_failed,
cast(countif(pass_flag) as float64) / nullif(cast(countif(completed_flag) as float64), 0) as pass_rate,
sum(violation_count) as total_violations,
avg(violation_count) as avg_violations_per_inspection,
max(violation_count) as max_violations_per_inspection,
avg(case when pass_flag then violation_count else null end) as avg_violations_per_pass,
avg(case when not pass_flag and completed_flag then violation_count else null end) as avg_violations_per_fail,
avg(case when completed_flag then violation_count else null end) as avg_violations_per_completed
from stage.inspection
group by 1,2,3