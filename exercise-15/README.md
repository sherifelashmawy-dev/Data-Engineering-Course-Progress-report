# Exercise 15: Real-Time Electricity Price Monitoring with Kafka

## Objective
Build a complete real-time data streaming pipeline using Apache Kafka to monitor electricity price changes, store the data, and visualize it in a dashboard.

## System Architecture
```
┌─────────────────┐      ┌──────────────┐      ┌─────────────────┐
│  Price Data     │      │    Kafka     │      │   Consumer      │
│  (JSON File)    │ ───> │    Topic     │ ───> │  (Save to CSV)  │
└─────────────────┘      └──────────────┘      └─────────────────┘
         │                      │                        │
         │                      │                        │
    Producer                  Broker                    │
   (Monitor)                                             │
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
- Auto-refresh capability
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

### 1. Start Kafka Infrastructure
```bash
docker-compose up -d
```

Verify containers:
```bash
docker ps
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

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
- Read messages from Kafka
- Save to `data/electricity_prices.csv`
- Display progress

### 5. Run Dashboard (New Terminal)
```bash
streamlit run streamlit_app.py
```

Access dashboard at: **http://localhost:8501**

## Kafka UI

Monitor Kafka topics and messages at: **http://localhost:8080**

## Testing the System

### Method 1: Simulate File Update
```bash
# Touch the file to trigger change detection
touch /Users/selashma20/Downloads/latest-prices.json
```

### Method 2: Update File Content
```bash
# Copy and modify the file
cp /Users/selashma20/Downloads/latest-prices.json /Users/selashma20/Downloads/latest-prices-backup.json
# Edit and save back
```

## Features

### Producer Features
✅ File change detection (MD5 hashing)  
✅ Automatic retry on failures  
✅ Timestamped messages  
✅ Partitioning by startDate  

### Consumer Features
✅ Persistent CSV storage  
✅ Auto-create output directory  
✅ Progress tracking  
✅ Graceful shutdown  

### Dashboard Features
✅ Real-time price monitoring  
✅ Auto-refresh every 10 seconds  
✅ Interactive Plotly charts  
✅ Hourly price analysis  
✅ Statistical summaries  
✅ Price distribution histogram  
✅ Configurable time range  

## Kafka Configuration

**Topic:** `electricity-prices`  
**Partitions:** 1  
**Replication Factor:** 1  
**Consumer Group:** `electricity-price-consumer`  

## Stopping the System
```bash
# Stop producer: Ctrl+C
# Stop consumer: Ctrl+C
# Stop Streamlit: Ctrl+C

# Stop Kafka infrastructure
docker-compose down
```

## Troubleshooting

**Issue:** Producer can't connect to Kafka  
**Solution:** Check if Kafka is running: `docker ps`

**Issue:** No data in dashboard  
**Solution:** Ensure consumer is running and has processed messages

**Issue:** File changes not detected  
**Solution:** Manually touch the file: `touch /path/to/latest-prices.json`

## Output Files

- `data/electricity_prices.csv` - All historical price data
- Docker volumes for Kafka/Zookeeper persistence

## Technology Stack

- **Message Broker:** Apache Kafka
- **Producer Library:** kafka-python
- **Consumer Library:** kafka-python
- **Visualization:** Streamlit + Plotly
- **Data Processing:** Pandas
- **Containerization:** Docker Compose

---

**Author:** Sherif Elashmawy  
**Date:** January 2026