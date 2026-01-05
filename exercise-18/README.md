# Exercise 18: Cloud Deployment to CSC cPouta

## Objective
Deploy a containerized Streamlit application to CSC's cPouta cloud infrastructure, demonstrating production cloud deployment skills.

## Deployment Summary

**Application**: Streamlit Population Visualization (Exercise 9)  
**Cloud Provider**: CSC cPouta (Finland's Academic Cloud)  
**OS**: Ubuntu 24.04 LTS  
**Container**: Docker  
**Public URL**: http://195.148.30.152:8503  

## What Was Deployed

- **Original App**: Exercise 09 - Streamlit Population Visualization
- **Improvements Made**:
  - ✅ Changed title from "Population of Nordics" to "Global Population Trends (1952-2007)"
  - ✅ Fixed bar chart title to "Global Population Stacked View"
  - ✅ Added data source attribution
  - ✅ Added section headers and descriptions
  - ✅ Improved chart formatting and user experience

## Setup Steps Completed

### 1. CSC Account Setup ✅
- Created CSC account at https://my.csc.fi/
- Applied for cPouta access
- Received approval

### 2. Virtual Machine Creation ✅
- **Instance Name**: FMI Streamlit Dashboard
- **Image**: Ubuntu 24.04 LTS
- **Flavor**: standard.large
- **Public IP**: 195.148.30.152
- **SSH Key**: csc-fmi-key.pem

### 3. Docker Installation ✅
- Docker version 28.2.2 installed on Ubuntu VM
- Docker service running and enabled

### 4. File Transfer ✅
Transferred 4 files from local machine to CSC VM via SCP:
- Dockerfile
- streamlit_population.py (updated version)
- requirements.txt
- population_country_columns.csv

### 5. Docker Build & Run ✅
```bash
# Built image
docker build -t streamlit-population-app .

# Ran container on port 8503 (avoiding conflict with port 8501)
docker run -d -p 8503:8501 --name population-viz streamlit-population-app
```

### 6. Security Group Configuration ✅
- Added ingress rule for port 8503
- Direction: Ingress
- Protocol: TCP
- Port: 8503
- CIDR: 0.0.0.0/0 (public access)

### 7. Application Verification ✅
- Accessed app at: http://195.148.30.152:8503
- All features working correctly
- Charts rendering properly
- Data loading successfully

## Technical Details

**Container Status:**
```
CONTAINER ID   IMAGE                         STATUS         PORTS
2207289aa494   streamlit-population-app      Up            0.0.0.0:8503->8501/tcp
```

**Ports Used:**
- 8503: Streamlit Population App (Exercise 18)
- 8082: Kafka UI (FMI project)
- 9092: Kafka Broker (FMI project)
- 2181: Zookeeper (FMI project)
- 22: SSH

## Key Learnings

✅ **Cloud VM Management**: Created and configured Ubuntu VM on CSC cPouta  
✅ **SSH Access**: Secure remote access using SSH keys  
✅ **Docker Deployment**: Built and ran containerized apps in cloud  
✅ **Port Configuration**: Managed multiple services on different ports  
✅ **Security Groups**: Configured firewall rules for external access  
✅ **File Transfer**: Used SCP for secure file transfer  
✅ **Production Debugging**: Fixed port conflicts and updated application  

## Commands Reference

### SSH to CSC VM
```bash
ssh -i ~/.ssh/csc-fmi-key.pem ubuntu@195.148.30.152
```

### Transfer Files
```bash
scp -i ~/.ssh/csc-fmi-key.pem <file> ubuntu@195.148.30.152:~/
```

### Docker Commands
```bash
# Build image
docker build -t streamlit-population-app .

# Run container
docker run -d -p 8503:8501 --name population-viz streamlit-population-app

# Check status
docker ps

# View logs
docker logs population-viz

# Stop container
docker stop population-viz

# Remove container
docker rm population-viz
```

## Troubleshooting

**Issue**: Port 8501 already in use  
**Solution**: Used port 8503 instead (port 8501 was occupied by another service)

**Issue**: Large Docker build context (3GB)  
**Solution**: Let build complete (inefficient but functional). For future: use .dockerignore

**Issue**: Cannot access app from browser  
**Solution**: Added security group rule for port 8503

## Production Considerations

✅ **Running**: Application is live and accessible  
✅ **Persistent**: Container runs in detached mode  
✅ **Isolated**: Separate from other services (FMI project)  
✅ **Secure**: Firewall rules control access

## Future Improvements

- Add .dockerignore to optimize build
- Use docker-compose for easier management
- Add SSL/HTTPS with reverse proxy
- Implement health checks
- Add monitoring and logging
- Use volume mounts for data persistence

## Technology Stack

- **Cloud**: CSC cPouta (OpenStack)
- **OS**: Ubuntu 24.04 LTS
- **Container**: Docker 28.2.2
- **Application**: Streamlit 1.40.0
- **Language**: Python 3.11
- **Data**: CSV (population data 1952-2007)

---

**Author:** Sherif Elashmawy  
**Date:** January 2026  
**Status**: ✅ Successfully deployed and verified in production
**Public URL**: http://195.148.30.152:8503
