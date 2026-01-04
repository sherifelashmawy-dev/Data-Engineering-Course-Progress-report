# Data Quality Plan: Training Data System

## C) Data Quality Measures

### Overview
Comprehensive data quality strategy to ensure accuracy, completeness, consistency, and reliability of training data.

---

## 1. Data Validation Rules

### Input Validation (JSON Level)

**Schema Validation:**
```json
{
  "required_fields": [
    "exercises[0].startTime",
    "exercises[0].duration",
    "exercises[0].sport"
  ],
  "optional_fields": [
    "exercises[0].distance",
    "exercises[0].calories",
    "exercises[0].samples.heartRate",
    "exercises[0].samples.recordedRoute"
  ]
}
```

**Field-Level Rules:**

| Field | Rule | Action on Failure |
|-------|------|-------------------|
| startTime | Valid ISO 8601 format | Reject file |
| duration | > 0 seconds, < 24 hours | Reject file |
| sport | Non-empty string | Set to "UNKNOWN" |
| distance | ≥ 0 meters | Set to NULL |
| calories | ≥ 0, < 10000 | Set to NULL |
| heartRate | 30-250 bpm | Remove invalid samples |
| latitude | -90 to 90 | Remove invalid points |
| longitude | -180 to 180 | Remove invalid points |

---

### Database-Level Validation

**Constraint Checks:**
```sql
-- Heart rate range
ALTER TABLE heart_rate_samples 
ADD CONSTRAINT chk_heart_rate 
CHECK (heart_rate_bpm BETWEEN 30 AND 250);

-- Geographic coordinates
ALTER TABLE gps_route_points 
ADD CONSTRAINT chk_latitude 
CHECK (latitude BETWEEN -90 AND 90);

ALTER TABLE gps_route_points 
ADD CONSTRAINT chk_longitude 
CHECK (longitude BETWEEN -180 AND 180);

-- Duration reasonableness
ALTER TABLE exercises 
ADD CONSTRAINT chk_duration 
CHECK (duration_seconds BETWEEN 60 AND 86400);

-- Distance reasonableness
ALTER TABLE exercises 
ADD CONSTRAINT chk_distance 
CHECK (distance_meters >= 0 AND distance_meters <= 500000);
```

---

## 2. Data Quality Dimensions

### Accuracy
**Definition:** Data correctly represents real-world values

**Measures:**
- Heart rate values within physiological range (30-250 bpm)
- GPS coordinates within valid geographic bounds
- Timestamps in logical sequence
- Calculated pace matches distance/time ratio

**Monitoring:**
```sql
-- Daily accuracy check
SELECT 
    DATE(upload_timestamp) as check_date,
    COUNT(*) as total_exercises,
    COUNT(CASE WHEN duration_seconds / 60.0 > distance_meters / 1000.0 * 30 
               THEN 1 END) as suspicious_pace_count,
    COUNT(CASE WHEN calories > duration_seconds / 60.0 * 50 
               THEN 1 END) as suspicious_calories_count
FROM exercises
WHERE upload_timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(upload_timestamp);
```

---

### Completeness
**Definition:** All required data fields are present

**Measures:**
- Percentage of exercises with heart rate data
- Percentage of exercises with GPS data
- Percentage of required fields populated

**Monitoring:**
```sql
-- Completeness dashboard
SELECT 
    COUNT(*) as total_exercises,
    COUNT(CASE WHEN distance_meters IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as pct_with_distance,
    COUNT(CASE WHEN calories IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as pct_with_calories,
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM heart_rate_samples hrs 
        WHERE hrs.exercise_id = exercises.exercise_id
    ) THEN exercise_id END) * 100.0 / COUNT(*) as pct_with_hr_data,
    COUNT(DISTINCT CASE WHEN EXISTS (
        SELECT 1 FROM gps_route_points gps 
        WHERE gps.exercise_id = exercises.exercise_id
    ) THEN exercise_id END) * 100.0 / COUNT(*) as pct_with_gps_data
FROM exercises
WHERE start_time >= CURRENT_DATE - INTERVAL '30 days';
```

**Targets:**
- 95% of exercises should have heart rate data
- 80% of outdoor exercises should have GPS data
- 100% of required fields populated

---

### Consistency
**Definition:** Data is consistent across system

**Measures:**
- Exercise duration matches heart rate sample timespan
- Distance matches GPS route calculation (±10%)
- User statistics match sum of exercises

**Monitoring:**
```sql
-- Consistency checks
WITH exercise_hr_duration AS (
    SELECT 
        e.exercise_id,
        e.duration_seconds as recorded_duration,
        EXTRACT(EPOCH FROM (MAX(hrs.timestamp) - MIN(hrs.timestamp))) as hr_duration
    FROM exercises e
    JOIN heart_rate_samples hrs ON e.exercise_id = hrs.exercise_id
    WHERE e.start_time >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY e.exercise_id, e.duration_seconds
)
SELECT 
    COUNT(*) as total_checked,
    COUNT(CASE WHEN ABS(recorded_duration - hr_duration) > 300 
               THEN 1 END) as inconsistent_duration_count
FROM exercise_hr_duration;
```

---

### Timeliness
**Definition:** Data is available when needed

**Measures:**
- Time from exercise completion to database insertion
- Statistics refresh frequency
- Dashboard update lag

**Monitoring:**
```sql
-- Data freshness check
SELECT 
    MAX(start_time) as latest_exercise,
    MAX(upload_timestamp) as latest_upload,
    EXTRACT(EPOCH FROM (NOW() - MAX(upload_timestamp))) / 3600 as hours_since_last_upload
FROM exercises;
```

**Targets:**
- 90% of exercises uploaded within 24 hours
- Statistics refreshed daily
- Dashboards updated within 1 hour of new data

---

### Uniqueness
**Definition:** No duplicate records

**Measures:**
- Duplicate exercise detection
- Duplicate user accounts

**Monitoring:**
```sql
-- Duplicate detection
SELECT 
    user_id,
    start_time,
    sport_type,
    duration_seconds,
    COUNT(*) as duplicate_count
FROM exercises
GROUP BY user_id, start_time, sport_type, duration_seconds
HAVING COUNT(*) > 1;
```

---

## 3. Quality Monitoring Dashboard

### Key Metrics

**Daily Quality Score:**
```
Quality Score = (Accuracy × 0.3) + 
                (Completeness × 0.3) + 
                (Consistency × 0.2) + 
                (Timeliness × 0.1) + 
                (Uniqueness × 0.1)
```

**Alerting Thresholds:**
- Quality score < 85%: Warning
- Quality score < 70%: Critical alert
- > 5% invalid records: Investigation required
- Statistics refresh delayed > 6 hours: Alert

---

## 4. Data Quality Workflow
```
┌─────────────────────────────────────┐
│  1. Pre-Ingestion Validation       │
│  - Schema check                     │
│  - Required fields                  │
│  - Format validation                │
└──────────┬──────────────────────────┘
           │ PASS ✓
           ▼
┌─────────────────────────────────────┐
│  2. Transformation Validation       │
│  - Range checks                     │
│  - Type conversions                 │
│  - Business rules                   │
└──────────┬──────────────────────────┘
           │ PASS ✓
           ▼
┌─────────────────────────────────────┐
│  3. Post-Load Validation            │
│  - Referential integrity            │
│  - Consistency checks               │
│  - Duplicate detection              │
└──────────┬──────────────────────────┘
           │ PASS ✓
           ▼
┌─────────────────────────────────────┐
│  4. Quality Metrics Calculation     │
│  - Update quality dashboard         │
│  - Check against thresholds         │
│  - Trigger alerts if needed         │
└─────────────────────────────────────┘
```

---

## 5. Error Handling Strategy

### Classification

**Level 1 - Critical:** Reject entire file
- Invalid JSON format
- Missing required fields
- Corrupt data

**Level 2 - Warning:** Accept with nulls
- Missing optional fields
- Out-of-range optional values
- Minor inconsistencies

**Level 3 - Info:** Accept with corrections
- Minor formatting issues
- Correctable data quality issues

### Quarantine Process
```
┌───────────────┐
│ Invalid File  │
└───────┬───────┘
        │
        ▼
┌───────────────────────┐
│ Move to Quarantine    │
│ /quarantine/YYYYMMDD/ │
└───────┬───────────────┘
        │
        ▼
┌───────────────────────┐
│ Log Error Details     │
│ - File name           │
│ - Error type          │
│ - Error message       │
│ - Timestamp           │
└───────┬───────────────┘
        │
        ▼
┌───────────────────────┐
│ Send Alert            │
│ - Email notification  │
│ - Dashboard flag      │
└───────────────────────┘
```

---

## 6. Data Quality Tests

### Automated Tests (Run Daily)
```sql
-- Test 1: Check for orphaned samples
SELECT COUNT(*) as orphaned_heart_rate_samples
FROM heart_rate_samples hrs
WHERE NOT EXISTS (
    SELECT 1 FROM exercises e 
    WHERE e.exercise_id = hrs.exercise_id
);

-- Test 2: Check for future timestamps
SELECT COUNT(*) as future_exercises
FROM exercises
WHERE start_time > NOW();

-- Test 3: Check for statistical outliers
SELECT user_id, COUNT(*) as exercise_count
FROM exercises
WHERE start_time >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY user_id
HAVING COUNT(*) > 10;  -- Suspicious: >10 exercises per day

-- Test 4: Check for GPS drift
WITH gps_distances AS (
    SELECT 
        exercise_id,
        ST_Distance(
            ST_MakePoint(lag_lon, lag_lat)::geography,
            ST_MakePoint(longitude, latitude)::geography
        ) / 1000 as km_per_hour
    FROM (
        SELECT 
            exercise_id,
            latitude,
            longitude,
            LAG(latitude) OVER (PARTITION BY exercise_id ORDER BY timestamp) as lag_lat,
            LAG(longitude) OVER (PARTITION BY exercise_id ORDER BY timestamp) as lag_lon,
            EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (PARTITION BY exercise_id ORDER BY timestamp))) / 3600 as hours
        FROM gps_route_points
    ) sub
)
SELECT COUNT(*) as suspicious_gps_points
FROM gps_distances
WHERE km_per_hour > 100;  -- Faster than 100 km/h
```

---

## 7. Reporting

### Daily Quality Report

**Email Template:**
```
Subject: Training Data Quality Report - [DATE]

Summary:
- Total exercises processed: X
- Quality score: XX%
- Invalid records: X (X%)
- Warnings: X
- Critical errors: X

Details:
- Completeness: XX%
- Accuracy: XX%
- Consistency: XX%

Actions Required:
[List of issues requiring attention]
```

### Weekly Quality Trends

- Quality score over time (7-day trend)
- Most common validation failures
- User data quality rankings
- System health indicators

---

## Implementation Priority

**Phase 1 (Week 1):**
✓ Implement basic schema validation  
✓ Set up database constraints  
✓ Create error logging  

**Phase 2 (Week 2):**
✓ Implement field-level validations  
✓ Set up quality metrics calculations  
✓ Create quality dashboard  

**Phase 3 (Week 3):**
✓ Implement automated tests  
✓ Set up alerting  
✓ Create reports  

**Phase 4 (Ongoing):**
✓ Monitor and refine thresholds  
✓ Add new quality checks as needed  
✓ Continuous improvement