# Exercise 16: Apache Airflow Setup and DAG Creation

## Objective
Install and configure Apache Airflow using Docker, explore example DAGs, and create a custom electricity price monitoring workflow.

## Setup Instructions

### 1. Download Airflow Docker Compose
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.1/docker-compose.yaml'
```

### 2. Create Required Directories
```bash
mkdir -p ./dags ./logs ./plugins ./config
```

### 3. Set Airflow User ID
```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 4. Initialize Database
```bash
docker compose up airflow-init
```

### 5. Start Airflow
```bash
docker compose up -d
```

## Configuration Changes

**Port Conflict Resolution:**
- Original port: 8080 (conflicted with Kafka UI from Exercise 15)
- Changed to: **8081**
- Modified in `docker-compose.yaml`: 
```yaml
  ports:
    - "8081:8080"
```

## Access Airflow

**URL:** http://localhost:8081

**Login Credentials:**
- Username: `airflow`
- Password: `airflow`

## Components Running

- **Webserver**: http://localhost:8081 (UI interface)
- **Scheduler**: Background DAG scheduling and task triggering
- **Worker**: Task execution engine
- **Triggerer**: Event-based trigger handler
- **PostgreSQL**: Metadata database (port 5432)
- **Redis**: Message broker for Celery (port 6379)

## Custom DAG Created

### Electricity Price Monitor DAG
**File:** `dags/electricity_price_dag.py`

**Purpose:** Demonstrates Airflow workflow concepts with a simple electricity price monitoring simulation

**Tasks:**
1. `start_monitoring` - BashOperator: Initialize monitoring session
2. `check_prices` - PythonOperator: Simulate price checking
3. `end_monitoring` - BashOperator: Complete monitoring session

**Workflow:**
```
start_monitoring → check_prices → end_monitoring
```

**Schedule:** Runs every hour (`timedelta(hours=1)`)

**Tags:** `electricity`, `monitoring`

**Owner:** sherif

## Running the Custom DAG

1. Navigate to http://localhost:8081
2. Find `electricity_price_monitor` in DAG list (may be paused)
3. Toggle the switch to **unpause** (activate) the DAG
4. Click on DAG name to view details
5. Click **Graph** tab to see workflow visualization
6. Click **▶ Play button** (top right) → "Trigger DAG" to run manually

## Monitoring DAG Execution

- **Graph View**: Visual workflow representation with task status
- **Grid View**: Task execution timeline and historical runs
- **Calendar View**: DAG run calendar
- **Gantt View**: Task duration visualization
- **Logs**: Click on task boxes to view execution logs
- **Code**: View DAG source code directly in UI

## Example DAGs Included

Airflow includes 53+ example DAGs demonstrating various concepts:
- `tutorial` - Basic Airflow concepts
- `example_bash_operator` - Bash command execution
- `example_python_operator` - Python function execution
- `example_branch_operator` - Conditional workflows
- `example_dynamic_task_mapping` - Dynamic task generation
- Dataset-based DAGs - Data-aware scheduling
- And many more...

**To activate example DAGs:** Click the toggle switch on the left of each DAG name

## Managing Airflow

### Stop Airflow
```bash
docker compose down
```

### Start Airflow
```bash
docker compose up -d
```

### View Logs
```bash
# Webserver logs
docker compose logs airflow-webserver

# Scheduler logs
docker compose logs airflow-scheduler

# Worker logs
docker compose logs airflow-worker
```

### Check Container Status
```bash
docker compose ps
```

### Restart Single Service
```bash
docker compose restart airflow-webserver
```

## Directory Structure
```
exercise-16/
├── docker-compose.yaml    # Airflow services configuration
├── .env                   # Environment variables (AIRFLOW_UID)
├── dags/                  # DAG definitions
│   └── electricity_price_dag.py
├── logs/                  # Airflow execution logs
├── plugins/               # Custom Airflow plugins
├── config/                # Additional configuration
└── README.md             # This file
```

## Key Concepts Learned

✅ **DAG (Directed Acyclic Graph)**: Workflow definition with tasks and dependencies  
✅ **Operators**: Task definitions (BashOperator, PythonOperator, etc.)  
✅ **Tasks**: Individual units of work in a DAG  
✅ **Dependencies**: Task execution order (`>>` operator)  
✅ **Scheduling**: Automated workflow execution with cron or timedelta  
✅ **Monitoring**: DAG and task status tracking through web UI  
✅ **Docker Compose**: Multi-container orchestration for Airflow  

## DAG Development Best Practices

1. **Start Date**: Always set in the past to avoid unexpected behavior
2. **Catchup**: Set to `False` to prevent backfilling historical runs
3. **Tags**: Use for organizing and filtering DAGs
4. **Retries**: Configure retry logic for fault tolerance
5. **Dependencies**: Use `>>` for readability
6. **Idempotency**: Design tasks to be safely re-runnable

## Troubleshooting

**Issue:** Port 8080 already in use  
**Solution:** Changed to port 8081 in docker-compose.yaml

**Issue:** DAG not appearing in UI  
**Solution:** Wait 30-60 seconds for Airflow to scan dags/ folder. Check scheduler logs for parsing errors.

**Issue:** Containers not starting  
**Solution:** Run `docker compose down` then `docker compose up -d`

**Issue:** Permission errors  
**Solution:** Ensure `.env` file has correct `AIRFLOW_UID=$(id -u)`

**Issue:** Database initialization failed  
**Solution:** Delete volumes and reinitialize: `docker compose down -v` then restart setup

## Technology Stack

- **Orchestration**: Apache Airflow 2.8.1
- **Containerization**: Docker Compose
- **Database**: PostgreSQL 13
- **Message Broker**: Redis latest
- **Python**: 3.8
- **Web Framework**: Flask (Airflow webserver)

## Resources

- **Official Documentation**: https://airflow.apache.org/docs/
- **Docker Deployment**: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/
- **DAG Authoring**: https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html

---

**Author:** Sherif Elashmawy  
**Date:** January 2026
