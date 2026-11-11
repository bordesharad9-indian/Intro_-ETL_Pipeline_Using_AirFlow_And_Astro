from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Settings: change these for your city 
LATITUDE = "51.5074"
LONGITUDE = "-0.1278"
POSTGRES_CONN_ID = "postgres_default"   # Airflow Connection (Postgres)
API_CONN_ID = "open_meteo_api"          # Airflow Connection (HTTP)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="eltweather",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),   # must be in the past
    schedule="@daily",                 # use schedule_interval on Airflow < 2.4
    catchup=False,
    tags=["weather", "elt"],
) as dag:

    @task
    def extract_weather_data():
        """Extract weather data from the Open-Meteo API."""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")
        endpoint = (
            f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}"
            f"&current_weather=true"
        )
        response = http_hook.run(endpoint)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        return response.json()

    @task
    def transform_weather_data(weather_data: dict):
        """Keep only the fields we want to store."""
        current = weather_data["current_weather"]
        return {
            "latitude": float(LATITUDE),
            "longitude": float(LONGITUDE),
            "temperature": current["temperature"],
            "windspeed": current["windspeed"],
            "winddirection": current["winddirection"],
            "weathercode": current["weathercode"],
        }

    @task
    def load_weather_data(data: dict):
        """Load the transformed data into Postgres."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        cursor.execute(
            """
            INSERT INTO weather_data
                (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                data["latitude"],
                data["longitude"],
                data["temperature"],
                data["windspeed"],
                data["winddirection"],
                data["weathercode"],
            ),
        )
        conn.commit()
        cursor.close()
        conn.close()

    # Task flow: extract -> transform -> load
    raw = extract_weather_data()
    clean = transform_weather_data(raw)
    load_weather_data(clean)