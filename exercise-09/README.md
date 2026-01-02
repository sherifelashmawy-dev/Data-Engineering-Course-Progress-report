# Exercise 09: Docker Streamlit Population Visualization

## Objective
Create a Docker image and container to run a Streamlit data visualization application.

## Application
Interactive web application for visualizing population data from countries worldwide:
- Line charts with selectable countries
- Stacked bar plots
- Interactive country selection

## Files
- `streamlit_population.py` - Streamlit application
- `population_country_columns.csv` - Population data (1952-2007)
- `Dockerfile` - Docker configuration
- `requirements.txt` - Python dependencies

## Building the Docker Image
```bash
# Build the image
docker build -t streamlit-population-app .

# Verify image was created
docker images | grep streamlit-population-app
```

## Running the Container
```bash
# Run the container
docker run -p 8501:8501 streamlit-population-app

# Run in detached mode (background)
docker run -d -p 8501:8501 --name population-viz streamlit-population-app

# View running containers
docker ps
```

## Accessing the Application

Once running, access the Streamlit app at:
```
http://localhost:8501
```

## Managing the Container
```bash
# Stop the container
docker stop population-viz

# Start the container
docker start population-viz

# Remove the container
docker rm population-viz

# View logs
docker logs population-viz
```

## Docker Commands Reference

### Build
```bash
docker build -t streamlit-population-app .
```

### Run
```bash
docker run -d -p 8501:8501 --name population-viz streamlit-population-app
```

### List & Manage
```bash
# List images
docker images

# List running containers
docker ps

# List all containers
docker ps -a

# Remove image
docker rmi streamlit-population-app

# View logs
docker logs -f population-viz
```

## Technology Stack
- **Base Image**: python:3.11-slim
- **Framework**: Streamlit
- **Libraries**: pandas, matplotlib
- **Port**: 8501

---

**Author:** Sherif Elashmawy  
**Date:** January 2026