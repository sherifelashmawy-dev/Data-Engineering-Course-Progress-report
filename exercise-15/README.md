# Exercise 15: Real-Time Electricity Price Monitoring with Kafka

## Objective
Build a complete real-time data streaming pipeline using Apache Kafka to monitor electricity price changes, store the data, and visualize it in a dashboard.

## Important Note

This exercise **reuses the existing Kafka infrastructure** from the FMI weather project:
- **Zookeeper**: Already running on port 2181
- **Kafka Broker**: Already running on port 9092  
- **Kafka UI**: Already running on http://localhost:8080

Therefore, we did **not** start new containers with the provided `docker-compose.yml`. Instead, the producer and consumer connect directly to the existing Kafka instance.

**Port Note:** Kafka UI uses port 8080, which is why Airflow (Exercise 16) was configured to use port 8081.

## System Architecture
```
┌─────────────────┐      ┌──────────────┐      ┌─────────────────┐
│  Price Data     │      │    Kafka     │      │   Consumer      │
│  (JSON File)    │ ───> │    Topic     │ ───> │  (Save to CSV)  │
└─────────────────┘      └──────────────┘      └─────────────────┘
         │                      │                        │
         │                      │                        │
    Producer                  Broker                    │
   (Monitor)              (localhost:9092)               │
                                                         ▼
                                               ┌─────────────────┐
                                               │   Streamlit     │
                                               │   Dashboard     │
                                               └─────────────────┘
```

## Components

### a) Producer (`producer.py`)
- Monitors electricity price JSON file for changes
- Detects file updates using MD5 hash comparison
- Sends price updates to Kafka topic
- Runs continuously checking every 10 seconds

### b) Consumer (`consumer.py`)
- Listens to Kafka topic for new price data
- Saves incoming messages to CSV file
- Maintains persistent storage of all price updates
- Runs continuously processing messages

### c) Visualization (`streamlit_app.py`)
- Real-time dashboard displaying price trends
- Interactive charts and statistics
- Auto-refresh capability every 10 seconds
- Hourly price analysis
- Distribution histograms

## Data Format

**Input (JSON):**
```json
{
  "prices": [
    {
      "price": 10.438,
      "startDate": "2026-01-04T22:45:00.000Z",
      "endDate": "2026-01-04T22:59:59.999Z"
    }
  ]
}
```

**Output (CSV):**
```
received_timestamp,price,startDate,endDate,source
2026-01-04T12:00:00,10.438,2026-01-04T22:45:00.000Z,2026-01-04T22:59:59.999Z,electricity-price-monitor
```

## Setup Instructions

### 1. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 2. Verify Kafka is Running
```bash
docker ps | grep kafka
```

You should see:
- `fmi-zookeeper` on port 2181
- `fmi-kafka` on port 9092
- `fmi-kafka-ui` on port 8080

### 3. Run Producer
```bash
python producer.py
```

The producer will:
- Monitor `/Users/selashma20/Downloads/latest-prices.json`
- Send updates when file changes
- Display status messages

### 4. Run Consumer (New Terminal)
```bash
python consumer.py
```

The consumer will:
- Read messages from Kafka topic `electricity-prices`
- Save to `data/electricity_prices.csv`
- Display progress

### 5. Run Dashboard (New Terminal)
```bash
streamlit run streamlit_app.py --server.port 8502
```

Access dashboard at: **http://localhost:8502**

**Note:** Port 8502 is used because port 8501 is occupied by the population visualization app from Exercise 09.

## Kafka UI

Monitor Kafka topics and messages at: **http://localhost:8080**

## Testing the System

### Trigger File Update
```bash
# Touch the file to trigger change detection
touch /Users/selashma20/Downloads/latest-prices.json
```

Watch what happens:
1. **Producer terminal** - Shows "File changed detected" and messages sent
2. **Consumer terminal** - Shows messages being saved  
3. **Dashboard** - Updates with new data

## Features

### Producer Features
✅ File change detection (MD5 hashing)  
✅ Automatic retry on failures  
✅ Timestamped messages  
✅ Partitioning by startDate  
✅ Continuous monitoring every 10 seconds

### Consumer Features
✅ Persistent CSV storage  
✅ Auto-create output directory  
✅ Progress tracking  
✅ Graceful shutdown  

### Dashboard Features
✅ Real-time price monitoring  
✅ Auto-refresh every 10 seconds  
✅ Interactive Plotly charts  
✅ Current price with delta from average
✅ Hourly price analysis  
✅ Statistical summaries  
✅ Price distribution histogram  
✅ Configurable time range (1-48 hours)
✅ Recent price data table

## Kafka Configuration

**Topic:** `electricity-prices`  
**Broker:** localhost:9092  
**Consumer Group:** `electricity-price-consumer`  

## Results

Successfully processed **193 electricity price records** through the Kafka streaming pipeline with 15-minute interval data from the electricity price feed.

## Stopping the System
```bash
# Stop producer: Ctrl+C
# Stop consumer: Ctrl+C
# Stop Streamlit: Ctrl+C

# Kafka infrastructure remains running for other projects
```

## Troubleshooting

**Issue:** Producer can't connect to Kafka  
**Solution:** Check if Kafka is running: `docker ps | grep kafka`

**Issue:** No data in dashboard  
**Solution:** Ensure consumer is running and has processed messages

**Issue:** File changes not detected  
**Solution:** Manually touch the file: `touch /Users/selashma20/Downloads/latest-prices.json`

**Issue:** Timezone mismatch error in Streamlit  
**Solution:** Already fixed - timestamps are parsed with `utc=True`

## Output Files

- `data/electricity_prices.csv` - All historical price data (193 records)
- Docker volumes for Kafka/Zookeeper persistence (shared with FMI project)

## Technology Stack

- **Message Broker:** Apache Kafka (reused from FMI project)
- **Producer Library:** kafka-python 2.0.2
- **Consumer Library:** kafka-python 2.0.2
- **Visualization:** Streamlit 1.40.0 + Plotly 5.18.0
- **Data Processing:** Pandas 2.1.4
- **Containerization:** Docker (existing infrastructure)

---

**Author:** Sherif Elashmawy  
**Date:** January 2026