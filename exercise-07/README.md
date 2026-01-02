# Exercise 07: USA Names Analysis

## Objective
Analyze USA baby names data from BigQuery to find the most common male and female name for every year, using action queries where data stays in BigQuery.

## Data Source
- **Dataset**: `bigquery-public-data.usa_names.usa_1910_current`
- **Description**: USA baby names from Social Security records
- **Coverage**: 1910 to current year

## Key Requirement
**Action Query**: Data is processed and saved entirely within BigQuery - never downloaded locally. This demonstrates efficient cloud data processing.

## Tasks Completed

### 1. Data Exploration
✅ Explored USA names dataset structure  
✅ Analyzed data coverage and statistics  

### 2. Action Query Execution
✅ Created SQL query to find most common names per year  
✅ Executed as CREATE TABLE query (data stays in BigQuery)  
✅ Saved results directly to BigQuery table  

### 3. Results Table Structure
Columns:
- `year` - Year of birth
- `most_common_male_name` - Top male name that year
- `male_name_count` - Number of boys with that name
- `most_common_female_name` - Top female name that year
- `female_name_count` - Number of girls with that name

### 4. Trend Analysis
✅ Identified names that dominated for multiple years  
✅ Analyzed decade-by-decade name changes  
✅ Found longest-running #1 names  

## Requirements
```bash
pip install -r requirements.txt
```

## Google Cloud Setup
- Project: data-analytics-project-482302
- BigQuery API enabled
- Credentials configured

## Usage
```bash
python usa_names_analysis.py
```

## Output
- **BigQuery Table**: `data-analytics-project-482302.usa_names_analysis.US_common_names`
- **CSV Sample**: `us_common_names_sample.csv`

## Technical Highlights

### Action Query Approach
```sql
CREATE OR REPLACE TABLE `destination` AS
SELECT ...
FROM `source`
```

Benefits:
- No data transfer to local machine
- Faster processing (runs on BigQuery servers)
- Cost-effective (no egress charges)
- Scalable for large datasets

## Sample Insights
- Male names tend to be more stable over decades
- Female naming trends change more frequently
- Regional and cultural influences visible in data

---

**Author:** Sherif Elashmawy  
**Date:** January 2026