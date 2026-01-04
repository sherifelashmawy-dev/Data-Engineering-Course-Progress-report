# Database Design: Multi-User Training Data System

## A) Data Model Design

### 1. Conceptual Data Model

**High-Level Entities and Relationships:**
```
USERS (1) ──→ (N) EXERCISES (1) ──→ (N) HEART_RATE_SAMPLES
                     │
                     └──→ (N) GPS_ROUTE_POINTS
                     
USERS (1) ──→ (1) USER_STATISTICS
EXERCISES (1) ──→ (1) EXERCISE_STATISTICS
```

**Key Entities:**
- **Users** - Athletes/participants in the study
- **Exercises** - Individual training sessions
- **Heart Rate Samples** - Time-series heart rate measurements
- **GPS Route Points** - Geographic location data
- **User Statistics** - Aggregated metrics per user
- **Exercise Statistics** - Computed metrics per exercise
- **Global Statistics** - System-wide aggregated metrics

---

### 2. Logical Data Model

**Entity Attributes:**

**USERS**
- user_id (PK)
- username
- email
- date_of_birth
- gender
- registration_date
- consent_given (boolean)

**EXERCISES**
- exercise_id (PK)
- user_id (FK → USERS)
- start_time
- duration_seconds
- sport_type (e.g., running, cycling, skiing)
- distance_meters
- calories
- upload_timestamp
- data_source (device model)

**HEART_RATE_SAMPLES**
- sample_id (PK)
- exercise_id (FK → EXERCISES)
- timestamp
- heart_rate_bpm

**GPS_ROUTE_POINTS**
- point_id (PK)
- exercise_id (FK → EXERCISES)
- timestamp
- latitude
- longitude
- altitude (optional)

**USER_STATISTICS** (Aggregated)
- stat_id (PK)
- user_id (FK → USERS)
- calculation_date
- total_exercises
- total_duration_hours
- total_distance_km
- total_calories
- avg_heart_rate
- avg_exercise_duration
- most_common_sport

**EXERCISE_STATISTICS** (Computed)
- stat_id (PK)
- exercise_id (FK → EXERCISES)
- avg_heart_rate
- max_heart_rate
- min_heart_rate
- avg_pace (min/km)
- elevation_gain (meters)
- route_distance_km

**GLOBAL_STATISTICS** (System-wide)
- stat_id (PK)
- calculation_date
- total_users
- total_exercises
- total_distance_all_users
- avg_exercises_per_user
- most_popular_sport

---

### 3. Physical Data Model

**SQL Schema for PostgreSQL:**
```sql
-- Users table
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    date_of_birth DATE,
    gender VARCHAR(20),
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    consent_given BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Exercises table
CREATE TABLE exercises (
    exercise_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    start_time TIMESTAMP NOT NULL,
    duration_seconds INTEGER,
    sport_type VARCHAR(50),
    distance_meters NUMERIC(10, 2),
    calories INTEGER,
    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_source VARCHAR(100),
    raw_json JSONB,  -- Store original JSON for reference
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Heart rate samples table (partitioned by date for performance)
CREATE TABLE heart_rate_samples (
    sample_id BIGSERIAL PRIMARY KEY,
    exercise_id INTEGER REFERENCES exercises(exercise_id) ON DELETE CASCADE,
    timestamp TIMESTAMP NOT NULL,
    heart_rate_bpm INTEGER,
    CHECK (heart_rate_bpm BETWEEN 30 AND 250)
) PARTITION BY RANGE (timestamp);

-- GPS route points table (partitioned by date)
CREATE TABLE gps_route_points (
    point_id BIGSERIAL PRIMARY KEY,
    exercise_id INTEGER REFERENCES exercises(exercise_id) ON DELETE CASCADE,
    timestamp TIMESTAMP NOT NULL,
    latitude NUMERIC(10, 8) NOT NULL,
    longitude NUMERIC(11, 8) NOT NULL,
    altitude NUMERIC(8, 2),
    CHECK (latitude BETWEEN -90 AND 90),
    CHECK (longitude BETWEEN -180 AND 180)
) PARTITION BY RANGE (timestamp);

-- User statistics table (materialized view)
CREATE TABLE user_statistics (
    stat_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id) ON DELETE CASCADE,
    calculation_date DATE NOT NULL,
    total_exercises INTEGER,
    total_duration_hours NUMERIC(10, 2),
    total_distance_km NUMERIC(10, 2),
    total_calories INTEGER,
    avg_heart_rate NUMERIC(5, 2),
    max_heart_rate INTEGER,
    avg_exercise_duration_minutes NUMERIC(8, 2),
    most_common_sport VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, calculation_date)
);

-- Exercise statistics table
CREATE TABLE exercise_statistics (
    stat_id SERIAL PRIMARY KEY,
    exercise_id INTEGER UNIQUE REFERENCES exercises(exercise_id) ON DELETE CASCADE,
    avg_heart_rate NUMERIC(5, 2),
    max_heart_rate INTEGER,
    min_heart_rate INTEGER,
    avg_pace_min_per_km NUMERIC(6, 2),
    elevation_gain_meters NUMERIC(8, 2),
    route_distance_km NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Global statistics table
CREATE TABLE global_statistics (
    stat_id SERIAL PRIMARY KEY,
    calculation_date DATE UNIQUE NOT NULL,
    total_users INTEGER,
    total_exercises INTEGER,
    total_distance_km NUMERIC(15, 2),
    avg_exercises_per_user NUMERIC(8, 2),
    most_popular_sport VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_exercises_user_id ON exercises(user_id);
CREATE INDEX idx_exercises_start_time ON exercises(start_time);
CREATE INDEX idx_exercises_sport_type ON exercises(sport_type);
CREATE INDEX idx_heart_rate_exercise_id ON heart_rate_samples(exercise_id);
CREATE INDEX idx_gps_exercise_id ON gps_route_points(exercise_id);
CREATE INDEX idx_user_stats_calculation_date ON user_statistics(calculation_date);
```

---

## Design Decisions

### Partitioning Strategy
- **Heart rate samples** and **GPS points** are partitioned by timestamp (monthly partitions)
- Reason: Massive data volume, improved query performance for time-based queries

### Normalization
- **3NF** (Third Normal Form) for core tables
- Separate statistics tables to avoid recalculation
- JSONB field for raw data preservation

### Data Types
- NUMERIC for precise measurements (distance, coordinates)
- INTEGER for counts and whole numbers
- TIMESTAMP for all time-related fields
- JSONB for flexible JSON storage

### Constraints
- Foreign keys with CASCADE delete
- CHECK constraints for data validation
- UNIQUE constraints for data integrity

### Performance Optimization
- Indexes on frequently queried columns
- Partitioning for time-series data
- Materialized views for statistics (optional)

---

## Storage Estimates

**Assumptions:**
- 100 users
- 10 exercises per user per month
- 1000 heart rate samples per exercise (average)
- 500 GPS points per exercise (average)

**Annual Storage:**
- Exercises: ~12,000 records × 1 KB = 12 MB
- Heart rate samples: ~12M records × 24 bytes = 288 MB
- GPS points: ~6M records × 32 bytes = 192 MB
- **Total: ~500 MB per year**

With 5 years of data: **~2.5 GB**