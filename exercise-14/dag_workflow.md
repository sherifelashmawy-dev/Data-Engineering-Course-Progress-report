# DAG Workflow: Training Data Pipeline

## B) Data Pipeline Architecture

### Overview
A **Directed Acyclic Graph (DAG)** workflow for processing training data from JSON files to database storage with statistics calculation.

---

## Workflow Diagram
```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA INGESTION LAYER                        │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
  [JSON Files]          [API Upload]         [Device Sync]
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ▼
                    ┌─────────────────────┐
                    │  1. File Validation │
                    │  - Check JSON format│
                    │  - Verify schema    │
                    │  - Detect duplicates│
                    └──────────┬──────────┘
                              ▼
                    ┌─────────────────────┐
                    │  2. Data Extraction │
                    │  - Parse JSON       │
                    │  - Extract metadata │
                    │  - Normalize data   │
                    └──────────┬──────────┘
                              ▼
                    ┌─────────────────────┐
                    │  3. Data Validation │
                    │  - Range checks     │
                    │  - Required fields  │
                    │  - Data types       │
                    └──────────┬──────────┘
                              ▼
                    ┌─────────────────────┐
                    │  4. Data Transform  │
                    │  - Calculate metrics│
                    │  - Enrich data      │
                    │  - Format timestamps│
                    └──────────┬──────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DATABASE LOADING LAYER                     │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
  [Insert User]      [Insert Exercise]    [Insert Samples]
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   STATISTICS CALCULATION LAYER                  │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
[Exercise Stats]      [User Stats]          [Global Stats]
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    VISUALIZATION & REPORTING                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## DAG Tasks (Apache Airflow Implementation)

### Task 1: `validate_json_files`
**Purpose:** Validate incoming JSON files  
**Dependencies:** None (start task)  
**Operations:**
- Check file format
- Verify JSON structure
- Validate against schema
- Check for duplicates
- Move invalid files to quarantine

**Outputs:** List of valid files

---

### Task 2: `extract_user_data`
**Purpose:** Extract and upsert user information  
**Dependencies:** `validate_json_files`  
**Operations:**
- Parse user metadata from JSON
- Check if user exists in database
- Create new user record if needed
- Update user information if changed

**Outputs:** user_id mapping

---

### Task 3: `extract_exercise_metadata`
**Purpose:** Extract exercise-level data  
**Dependencies:** `extract_user_data`  
**Operations:**
- Parse exercise metadata (duration, sport, distance)
- Store raw JSON in JSONB field
- Insert into exercises table
- Return exercise_id

**Outputs:** exercise_id list

---

### Task 4: `load_heart_rate_samples` (Parallel)
**Purpose:** Load heart rate time series  
**Dependencies:** `extract_exercise_metadata`  
**Operations:**
- Extract heart rate samples array
- Batch insert into heart_rate_samples table
- Validate heart rate ranges (30-250 bpm)

**Outputs:** Sample count per exercise

---

### Task 5: `load_gps_route_points` (Parallel)
**Purpose:** Load GPS coordinates  
**Dependencies:** `extract_exercise_metadata`  
**Operations:**
- Extract GPS route array
- Validate lat/long ranges
- Batch insert into gps_route_points table

**Outputs:** Point count per exercise

---

### Task 6: `calculate_exercise_statistics`
**Purpose:** Compute per-exercise stats  
**Dependencies:** `load_heart_rate_samples`, `load_gps_route_points`  
**Operations:**
- Calculate avg/max/min heart rate
- Calculate pace from distance and time
- Calculate elevation gain from GPS
- Insert into exercise_statistics table

**Outputs:** Exercise stats records

---

### Task 7: `update_user_statistics`
**Purpose:** Aggregate user-level stats  
**Dependencies:** `calculate_exercise_statistics`  
**Operations:**
- Sum total exercises, distance, calories per user
- Calculate averages
- Identify most common sport
- Upsert into user_statistics table

**Outputs:** User stats records

---

### Task 8: `update_global_statistics`
**Purpose:** Calculate system-wide metrics  
**Dependencies:** `update_user_statistics`  
**Operations:**
- Count total users and exercises
- Calculate global aggregates
- Identify trends
- Insert into global_statistics table

**Outputs:** Global stats record

---

### Task 9: `generate_reports`
**Purpose:** Create visualization outputs  
**Dependencies:** `update_global_statistics`  
**Operations:**
- Generate user dashboards
- Create trend charts
- Export summary reports
- Send notifications (optional)

**Outputs:** Report files, dashboard updates

---

### Task 10: `cleanup_and_archive`
**Purpose:** Post-processing cleanup  
**Dependencies:** `generate_reports`  
**Operations:**
- Archive processed JSON files
- Clean up temporary files
- Log processing summary
- Update processing metadata

**Outputs:** Processing logs

---

## Airflow DAG Configuration
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'training_data_pipeline',
    default_args=default_args,
    description='Process Polar training data from JSON to database',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
)

# Define tasks...
```

---

## Schedule Strategy

**Daily Processing:**
- Run at 2:00 AM when system load is low
- Process all new files from previous day
- Allow 4-hour window for completion

**Batch Size:**
- Process up to 1000 exercises per run
- Split large batches into smaller chunks
- Parallel processing where possible

**Error Handling:**
- Retry failed tasks up to 3 times
- Alert on persistent failures
- Quarantine problematic files

---

## Data Flow Example

**Input:** Polar JSON file with 1 training session
```
1. validate_json_files → ✓ Valid
2. extract_user_data → user_id=123 (existing)
3. extract_exercise_metadata → exercise_id=5001
4. load_heart_rate_samples (parallel) → 847 samples inserted
5. load_gps_route_points (parallel) → 421 points inserted
6. calculate_exercise_statistics → avg HR=145, max=182
7. update_user_statistics → total exercises=127, total distance=1,245 km
8. update_global_statistics → total exercises=15,342
9. generate_reports → User dashboard updated
10. cleanup_and_archive → JSON archived to /processed/2026/01/
```

**Processing Time:** ~45 seconds per exercise

---

## Scalability Considerations

- **Horizontal scaling:** Add more Airflow workers
- **Database:** Use read replicas for statistics queries
- **Caching:** Redis for frequently accessed stats
- **Partitioning:** Time-based partitions for large tables