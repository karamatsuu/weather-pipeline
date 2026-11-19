from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from pendulum import datetime
import requests


@dag(
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    tags=["weather"],
)
def weather_pipeline():

    # ---------- EXTRACT: go get the data ----------
    @task
    def extract_weather():
        cities = {
            "Tashkent": (41.31, 69.24),
            "London": (51.51, -0.13),
            "Tokyo": (35.68, 139.69),
            "Nukus": (42.46, 59.61),
            "Almaty": (43.24, 76.89),
            "Bishkek": (42.87, 74.59),
        }

        results = []
        for city, (lat, lon) in cities.items():
            url = (
                f"https://api.open-meteo.com/v1/forecast?"
                f"latitude={lat}&longitude={lon}"
                f"&daily=temperature_2m_mean&timezone=auto"
            )
            response = requests.get(url)
            data = response.json()
            temperature = data["daily"]["temperature_2m_mean"][
                0
            ]  # today's mean, in local time
            results.append({"city": city, "temperature": temperature})
            print(f"{city}: {temperature}°C (daily mean)")

        return results

    # ---------- TRANSFORM: tidy / enrich the data ----------
    @task
    def transform_weather(weather_data: list) -> list:
        # Sort cities from coldest to warmest
        sorted_data = sorted(weather_data, key=lambda row: row["temperature"])

        # Add a "freezing" flag (True if temperature is 0°C or below)
        for row in sorted_data:
            row["freezing"] = row["temperature"] <= 0
            print(
                f"{row['city']}: {row['temperature']}°C (freezing: {row['freezing']})"
            )

        return sorted_data

    # ---------- LOAD: save the data to a file ----------
    @task
    def load_weather(weather_data: list) -> None:
        hook = PostgresHook(postgres_conn_id="weather_db")

        # 1) Create the table if it doesn't exist yet
        hook.run("""
            CREATE TABLE IF NOT EXISTS weather_history (
                id SERIAL PRIMARY KEY,
                city TEXT,
                temperature REAL,
                freezing BOOLEAN,
                recorded_at TIMESTAMP DEFAULT NOW()
            );
        """)

        # 2) Insert one row per city
        for row in weather_data:
            hook.run(
                "INSERT INTO weather_history (city, temperature, freezing) VALUES (%s, %s, %s);",
                parameters=(row["city"], row["temperature"], row["freezing"]),
            )

        print(f"Inserted {len(weather_data)} rows into weather_history")

    @task
    def report_weather() -> None:
        hook = PostgresHook(postgres_conn_id="weather_db")
        records = hook.get_records("""
            SELECT city,
                   ROUND(AVG(temperature)::numeric, 1) AS avg_temp,
                   COUNT(*) AS readings
            FROM weather_history
            GROUP BY city
            ORDER BY avg_temp ASC;
        """)
        print("Average temperature per city (coldest first):")
        for city, avg_temp, readings in records:
            print(f"  {city}: {avg_temp}°C  ({readings} readings)")

    # ---------- WIRING: extract -> transform -> load ----------
    data = extract_weather()
    transformed = transform_weather(data)
    loaded = load_weather(transformed)
    loaded >> report_weather()


weather_pipeline()
