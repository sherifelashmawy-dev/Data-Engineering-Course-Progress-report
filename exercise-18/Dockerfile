FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY streamlit_population.py .
COPY population_country_columns.csv .

EXPOSE 8501

CMD ["streamlit", "run", "streamlit_population.py", "--server.port=8501", "--server.address=0.0.0.0"]