# Weather ETL with Airflow and Docker

## Overview
This project sets up an Airflow DAG to orchestrate an ETL pipeline that fetches, transforms, and loads weather data into BigQuery. The DAG extracts data from the OpenWeatherMap API, transforms it, and loads it into a data warehouse for further analysis.

### Prerequisites
- **Docker**: Install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/).
- **Git**

---

## Project Structure
```
/home/weather-etl-bigquery-peter/
.github/workflows/ci.yml        # CI/CD pipeline configuration
configs/                         # Configuration files
dags/                            # Airflow DAG files
helpers/                         # Helper scripts and utilities
tests/                           # Unit and integration tests
var/                             # Airflow logs and other variable data
images/                          # Images for documentation
.dockerignore                    # Files to ignore in Docker builds
.gitignore                       # Files to ignore in Git
.pre-commit-config.yaml          # Pre-commit configuration for linting
docker-compose.yml               # Docker Compose file for setting up services
Dockerfile                       # Dockerfile for building the Airflow image
poetry.lock                      # Poetry lockfile for dependencies
pyproject.toml                   # Poetry configuration file
```

---

## Setup Guide

### 1. Clone the Repository
```bash
git clone https://github.com/Data-Epic/Weather-ETL-Bigquery-Peter.git
cd Weather-ETL-BigQuery-Peter
```

### 2. Configure OpenWeatherMap API
Sign up for an API key from [OpenWeatherMap](https://openweathermap.org/api) and update the `.env` file with your API key.

### 3. Configure CountryStateCity API
Sign up for an API key from [CountryStateCity](https://countrystatecity.in/) and update the `.env` file with your API key.

### 4. Create and add Environment Variables
```bash
touch .env
```

Edit the `.env` file and configure the following variables:
```bash
# .env

# Airflow configurations
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW_DATABASE_SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_UID=50000
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__WEBSERVER__WEB_SERVER_WORKER_TIMEOUT=400
AIRFLOW__TRIGGERER__DEFAULT_CAPACITY=1000
AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300

# Google Cloud Credentials
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/secret.json

# API Key
OPENWEATHER_API_KEY=your_openweathermap_api_key
COUNTRY_CITY_API_KEY=countrystatecity_api_key

# BigQuery Configuration
BIGQUERY_PROJECT_ID=your_gcp_project_id
BIGQUERY_DATASET_ID=your_bigquery_dataset
BIGQUERY_TABLE_LOCATION=your_location_table_name
BIGQUERY_TABLE_DATE=your_date_table_name
BIGQUERY_TABLE_WEATHER_FACT=your_fact_table_name
BIGQUERY_TABLE_WEATHER_TYPE=your_weather_type_table
```

### 5. Build and Run the Docker Services
Run the following command to start the services:
```bash
docker compose up --build
```
This will launch:
- **Airflow Webserver**: Accessible at [http://localhost:8080](http://localhost:8080).
- **PostgreSQL Database**: Running on port `5432`.

### 6. Accessing Airflow and Setting Up the DAG
1. Open [http://localhost:8080](http://localhost:8080) in your browser.
2. Log in with the default credentials (`airflow`/`airflow`).
3. Add your cities and countries in **Admin > Variables** (e.g., key: `CITY_NAMES`, value: `Abuja, Ife, Lagos, Accra, Pretoria`) (e.g., key: `COUNTRY_NAMES`, value: `Nigeria, South Africa, Ghana`).

**Airflow UI Variables:**
![Setting Environment Variables](images/dag_variables.jpg)

4. Navigate to the DAGs section to manually trigger `weather_etl_dag`, or wait for it to run according to its schedule to begin and fetching and loading data.

  **Airflow DAG Execution:**

  ![Airflow DAG Execution](images/dags.jpg)

### 7. Stopping and Cleaning Up Services
To stop the services:
```bash
docker compose down
```

To clean up containers, volumes, and images:
```bash
docker compose down --volumes --rmi all
```

---

## Docker Compose File Breakdown
The `docker-compose.yml` defines the following services:

1. **PostgreSQL**: Database for storing ETL, Airflow data and metadata.
2. **Airflow Init**: Initializes the Airflow database.
3. **Airflow Webserver**: Airflow UI accessible at [http://localhost:8080](http://localhost:8080).
4. **Airflow Scheduler**: Triggers scheduled workflows.

## Additional Notes
- The PostgreSQL database is accessible on port `5432`. You can connect using the credentials from `.env`.
- Modify the Airflow UI variables depending on your need.
- Modify Airflow configurations by editing the `.env` or `docker-compose.yml` file depending on your need.

---


## Step-by-Step Guide: How Data is Fetched from the API and Loaded into BigQuery

### 1. Extraction of Country Codes from the Rest Countries API
The goal is to extract current weather information from the OpenWeather API for multiple cities, such as Abuja, London, and Cairo, and load this data into a database. The API request can fetch data for specified cities using parameters like city name, state code, and country code. (Note: Searching by state is only available for U.S. locations.)

**API Calls:**
- `https://api.openweathermap.org/data/2.5/weather?q={city name}&appid={API key}`
- `https://api.openweathermap.org/data/2.5/weather?q={city name},{country code}&appid={API key}`
- `https://api.openweathermap.org/data/2.5/weather?q={city name},{state code},{country code}&appid={API key}`

Since the API doesn’t allow fetching data for multiple cities simultaneously, each city’s data must be fetched one at a time. For example, to extract data for Abuja, London, and Cairo, each city must be paired with its respective country code.

To obtain these country codes, the country names where the cities are located are passed as arguments to the Rest Countries API.

**API Call:**
- `https://restcountries.com/v3.1/name/{country}`

**Example API Response for Nigeria:**
```json
{
  "name": {
    "common": "Nigeria",
    "official": "Federal Republic of Nigeria",
    "nativeName": {
      "eng": {
        "official": "Federal Republic of Nigeria",
        "common": "Nigeria"
      }
    }
  },
  "tld": [".ng"],
  "cca2": "NG"
}
```
The `cca2` key represents the country code, which is extracted for use in the OpenWeather API calls.

### 2. Extraction of Geographical Information from the OpenWeather API
With the country codes obtained, the next step is to fetch the geographical information (latitude and longitude) for each city using the OpenWeather API.

**API Call Example for Abuja (Nigeria):**
- `https://api.openweathermap.org/data/2.5/weather?q=abuja,ng&appid={API key}`

**Example API Response:**
```json
{
  "name": "Abuja",
  "local_names": {"az": "Abuca", "fa": "آبوجا", ...},
  "lat": 9.0643305,
  "lon": 7.4892974,
  "country": "NG",
  "state": "Federal Capital Territory"
}
```
This returns the city’s name, local names, country, latitude, and longitude.

### 3. Extraction of Current Weather Data from the OpenWeather API
The main data to be extracted—current weather conditions—is fetched using the latitude and longitude obtained in the previous step.

**API Call:**
- `https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={part}&appid={API key}`

**Parameters:**
- `lat`: Latitude, decimal (-90; 90).
- `lon`: Longitude, decimal (-180; 180).
- `appid`: The unique API key.
- `exclude`: Optional. Excludes certain parts of the weather data (e.g., minutely, hourly, daily, alerts).

An Airflow environment variable `WEATHER_FIELDS_EXCLUDE` is used to exclude parts not needed (e.g., `minutely,hourly,daily,alerts`), retaining only `current` data.

**Example API Call for Abuja:**
- `https://api.openweathermap.org/data/3.0/onecall?lat=9.07&lon=7.49&exclude=hourly,daily,alerts,minutely&appid={API key}`

**Example API Response:**
```json
{
  "lat": 9.07,
  "lon": 7.49,
  "timezone": "Africa/Lagos",
  "timezone_offset": 3600,
  "current": {
    "dt": 1726854764,
    "sunrise": 1726809568,
    "sunset": 1726853243,
    "temp": 297.5,
    ...
  }
}
```
This returns fields such as current weather data, latitude, longitude, and timezone for the specified city.

### 4. Merging Current Weather Data and Geographical Data
The extracted current weather data is merged with the geographical information to create a comprehensive dataset useful for analytics.

**Merged Data Example:**
```json
{
  "lat": 6.46,
  "lon": 3.39,
  "timezone": "Africa/Lagos",
  "timezone_offset": 3600,
  "current": { "dt": 1726747705, "sunrise": 1726724175, ... },
  "city": "Lagos",
  "country": "Nigeria",
  "state": "Lagos"
}
```
For each city specified in the Airflow configuration, the data is extracted and merged.

### 5. Transforming the Merged Data
The merged data is transformed into a list of dictionaries, where each dictionary represents the processed weather data for each city. This data is structured to be easily loaded into a PostgreSQL database.

**Example Transformed Record:**
```json
{
  "city": "Lagos",
  "country": "NG",
  "state": "Lagos State",
  "latitude": 6.46,
  "longitude": 3.39,
  "timezone": "Africa/Lagos",
  ...
}
```

### 6. Data Modeling and Loading into BigQuery (Final Step)

#### Data Modeling
The processed weather records are now loaded into their modeled respective tables utilizing a 3NF into Bigquery, It leverages a delete-write pattern to avoid duplicates. A Bigquery sandbox account was setup.

**Database Model Diagram:**

![DB Model Diagram](images/db_model.png)

# Weather ETL DAG Explanation

1. DAG Definition:
The DAG is defined with the following parameters:
- Start date: October 27, 2024
- schedule interval: 1 hour
- Description: "Weather ETL DAG that fetches weather data from the OpenWeather API, transforms the data and loads it into Bigquery"
- Tags: ['weather']
- Max active runs: 1
- Render template as native object: True

2. Task Breakdown:

a) get_country_code:
- Retrieves country codes for the specified countries.
- Returns a dictionary with status, message, and country codes.

b) get_weather_fields:
- Retrieves retrieve weather fields from the OpenWeatherMap API using the country code and validated city name that will be validated from the country city API
- Returns a dictionary containing the status of the operation,
  a message and the weather data

c) restructure_geographical_data:
- Extracts relevant fields from the geographical records.
- Returns a dictionary with geographical fields and the longitude/latitude data.

d) merge_current_weather_data:
- Combines the current weather data from the API with the previously retrieved country and state information.
- Returns a list of dictionaries with complete current weather information for each city.

e) transform_weather_records:
- Transforms the weather records into a more structured format.
- Converts timestamps to datetime objects and selects specific fields.

f) load_records_to_location_dim:
-  loads the transformed weather records into the Location dimension table in Bigquery.
- It also checks if the records already exist in the weather fact table. If the records do not exist in the fact table, it inserts the records into the fact table.
- It does not load existing records in the table to avoid duplicates.

g) load_records_to_weather_type_dim:
-  loads the transformed weather records into the weather type dimension table in Bigquery.
- It also checks if the records already exist in the weather fact table. If the records do not exist in the fact table, it inserts the records into the fact table.
- It does not load existing records in the table to avoid duplicates.

h) create_date_dim:
- Loads the transformed weather records into the Date Dimension table in BigQuery.
- It also checks if the records already exist in the date dimension table. If the records do not exist in the date dimension table, it inserts the records into the date dimension
- It does not load existing records in the table to avoid duplicates.

i) join_date_dim_with_weather_fact:
- Joins the Date Dimension table with the Weather Fact table in Bigquery.
- It checks if the records already exist in the date dimension table and the weather fact table.
- If the records exist in both tables, it joins the records by updating the date_id field in the weather fact table.


3. DAG Structure:

The DAG is structured as follows:

```python


with DAG(
    dag_id="weather_etl_dag_hourly",
    start_date=datetime(2024, 10, 29),
    schedule_interval=timedelta(hours=1),
    description="Weather ETL DAG that fetches weather data from the OpenWeather API, transforms the data and loads it into BigQuery",
    catchup=False,
    tags=["weather"],
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    create_dataset_task = PythonOperator(
        task_id="create_bigquery_dataset",
        python_callable=create_bigquery_dataset,
    )

    get_country_codes_task = PythonOperator(
        task_id="get_country_codes",
        python_callable=retrieve_country_codes,
    )

    retrieve_weather_fields_task = PythonOperator(
        task_id="retrieve_weather_fields",
        python_callable=retrieve_weather_fields,
    )

    restructure_weather_fields_task = PythonOperator(
        task_id="restructure_weather_fields",
        python_callable=restructure_weather_fields,
    )

    merge_weather_data_task = PythonOperator(
        task_id="merge_weather_data",
        python_callable=merge_weather_data,
    )

    transform_records_task = PythonOperator(
        task_id="transform_records",
        python_callable=transform_records,
    )

    load_location_dim_task = PythonOperator(
        task_id="load_location_dim",
        python_callable=load_location_dim,
    )

    load_weather_type_dim_task = PythonOperator(
        task_id="load_weather_type_dim",
        python_callable=load_weather_type_dim,
    )

    load_date_dim_task = PythonOperator(
        task_id="load_date_dim",
        python_callable=load_date_dim,
    )

    join_date_dim_with_fact_task = PythonOperator(
        task_id="join_date_dim_with_fact",
        python_callable=join_date_dim_and_fact,
    )

    (
        create_dataset_task
        >> get_country_codes_task
        >> retrieve_weather_fields_task
        >> restructure_weather_fields_task
        >> merge_weather_data_task
        >> transform_records_task
        >> load_location_dim_task
        >> load_weather_type_dim_task
        >> load_date_dim_task
        >> join_date_dim_with_fact_task
    )

```

4. How the DAG works:

  - The DAG starts by getting country codes for the specified countries.
  - It then fetches geographical information for the specified cities using these country codes.
  - The geographical fields are extracted and separated into two parts: geographical information (city, country, state) and longitude/latitude data.
  - The current weather data from the API is merged with the country and state information.
  - The merged current weather records are then transformed into a more structured format.
  - It then loads the transformed weather records into the Location dimension table in Bigquery. It also checks if the corresponding records already exist in the weather fact table. If the records do not exist in the fact table, it inserts the records into the fact table. If the corresponding records exist in the fact table, it overwrites the records in the fact table.
  - It loads the transformed weather records into the Weather Type Dimension table in the BigQuery. It also checks if the corresponding records already exist in the fact table. If the records do not exist in the fact table, it inserts the records in the fact table. If the corresponding records exist in the fact table, it overwrites the records in the fact table.
  - It loads the transformed weather records into the Date Dimension table in BigQuery. It also checks if the records already exist in the date dimension table. If the records do not exist in the date dimension table, it inserts the records into the date dimension.
  - It now joins the Date Dimension table with the Weather Fact table in BigQuery. It checks if the records already exist in the date dimension table and the weather fact table. If the records exist in both tables, it joins the records by updating the date_id field in the weather fact table.
  - All dimension tables in the Bigquery dataset, do not accept duplicates nor can be overwritten when the airflow DAG reruns, only the fact tables can overwritten.

Configuration
The DAG uses several configuration variables:

1. AIRFLOW_COUNTRY_NAMES: List of country names to fetch weather data for.
2. AIRFLOW_CITY_NAMES: List of city names to fetch weather data for.
3. AIRFLOW_FIELDS: List of geographical fields to retrieve from the API.
4. AIRFLOW_WEATHER_FIELDS_EXCLUDE: Weather fields to exclude from the API response.
5. AIRFLOW_API_KEY: OpenWeather API key.
6. AIRFLOW_COUNTRY_CITY_API_KEY: CountryStateCity API key.
6. AIRFLOW_START_DATE_YEAR: The year to start creating date records from
7. AIRFLOW_END_DATE_YEAR: The year to stop creating date records

The DAG is scheduled to run every hour, ensuring that the Bigquery dataset is regularly updated with the latest weather information for the specified cities.

**Airflow UI Variables:**
![Airflow UI Variables](images/dag_variables.jpg)


**DAG Worfklow:**
![Airflow DAG Success](images/dags.jpg)

5. Tables Querying in BigQuery

  - Location Table Query Result:

  ![Location Table Query](images/query1.jpg)

  - Weather Table Query Result:

  ![Weather Table Query](images/query3.jpg)

  - Date Table Query Result:

  ![Date Table Query](images/query2.jpg)
