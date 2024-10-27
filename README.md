# Weather-ETL-Peter

# Weather ETL with Airflow and Docker

## Overview
This project sets up an Airflow DAG to orchestrate an ETL pipeline that fetches, transforms, and loads weather data into a PostgreSQL database. The DAG extracts data from the OpenWeatherMap API, transforms it, and loads it into a database for further analysis.

### Prerequisites
- **Docker**: Install [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/).
- **OpenWeatherMap API**: Get your free API key [here](https://openweathermap.org/api).

---

## Project Structure
```
/home/etl_ochestration_peter/
├── dags/                    # Airflow DAGs and scripts
│   ├── weather_dag.py       # Main Airflow DAG for ETL
│   ├── config.py            # Project configuration
│   ├── logger_config.py     # Logging configuration
│   ├── utils.py             # Helper functions
│   ├── database.py          # Database operations and connection
│   └── models.py            # Database schema definitions
├── tests/                   # Project test files
├── Dockerfile               # Docker image definition
├── docker-compose.yml       # Docker services definition
├── requirements.txt         # Python dependencies
├── pyproject.toml           # Poetry project configuration
├── poetry.lock              # Locked dependencies for consistency
├── .pre-commit-config.yaml  # Pre-commit hooks configuration
├── README.md                # This README file
└── images/                  # Images for documentation
    └── weather_erd.png      # Weather database ERD
```

---

## Setup Guide

### 1. Clone the Repository
```bash
git clone https://github.com/Data-Epic/Weather-ETL-Peter.git
cd Weather-ETL-Peter
```

### 2. Set Up Directories and Environment Variables
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Edit the `.env` file and configure the following variables:
```bash
# .env

# Airflow and PostgreSQL configurations
AIRFLOW_UID=your_uid              # Use 'id -u' to get your UID
DB_USER=myadmin                   # PostgreSQL username
DB_PASSWORD=mypassword            # PostgreSQL password
DB_NAME=weather_etl               # Database name
DB_URL=postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@weather-db/${DB_NAME}

# OpenWeatherMap API Key
API_KEY=your_weather_api_key
```

### 3. Configure OpenWeatherMap API
Sign up for an API key from [OpenWeatherMap](https://openweathermap.org/api) and update the `.env` file with your API key.

### 4. Build and Run the Docker Services
Run the following command to start the services:
```bash
docker compose up --build
```
This will launch:
- **Airflow Webserver**: Accessible at [http://localhost:8080](http://localhost:8080).
- **PostgreSQL Database**: Running on port `5432`.

### 5. Accessing Airflow and Setting Up the DAG
1. Open [http://localhost:8080](http://localhost:8080) in your browser.
2. Log in with the default credentials (`airflow`/`airflow`).
3. Add your cities and countries in **Admin > Variables** (e.g., key: `CITIES`, value: `London, Ife, Lagos`).

![Setting Environment Variables](images/dag_variables.jpg)

4. Navigate to the DAGs section to manually trigger `weather_etl_dag`, or wait for it to run according to its schedule.

![Airflow DAG Execution](images/dag_dag.jpg)

### 6. Stopping and Cleaning Up Services
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

1. **PostgreSQL**: Database for storing ETL data.
2. **Airflow Init**: Initializes the Airflow database.
3. **Airflow Webserver**: Airflow UI accessible at [http://localhost:8080](http://localhost:8080).
4. **Airflow Scheduler**: Triggers scheduled workflows.
5. **Airflow Triggerer**: Manages long-running tasks for scalability.

### Running the Docker Compose
1. Ensure you're in the project directory with the `docker-compose.yml` file.
2. Build and start the containers:
    ```bash
    docker compose up --build
    ```

### Stopping Services
Stop the services:
```bash
docker compose down
```

Clean up:
```bash
docker compose down -v
```

---

## Additional Notes
- Airflow logs are stored in the `./logs` directory for debugging.
- The PostgreSQL database is accessible on port `5432`. You can connect using the credentials from `.env`.
- Modify Airflow configurations by editing the `.env` or `docker-compose.yml` file.

---

This optimized README now gives clear, concise instructions on setting up the project, running it, and handling services with Docker.

## Step-by-Step Guide: How Data is Fetched from the API

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

### 6. Loading into PostgreSQL Database (Final Step)
The processed weather records are loaded into the database using a delete-write pattern to avoid duplicates. A cloud PostgreSQL database was set up on Render, and the data is inserted for use in analytics.

**Database Model Diagram:**

![DB Model Diagram](images/db_model_diagram.png)

# Weather ETL DAG Explanation

1. DAG Definition:
The DAG is defined with the following parameters:
- Start date: September 19, 2024
- schedule interval: 1 hour
- Description: "Weather ETL DAG that fetches weather data from the OpenWeather API, transforms the data and loads it into a Postgres database"
- Tags: ['weather']
- Max active runs: 1
- Render template as native object: True

2. Task Breakdown:

a) get_country_code:
- Retrieves country codes for the specified countries.
- Returns a dictionary with status, message, and country codes.

b) get_geographical_data:
- Fetches current weather information for specified cities using country codes.
- Returns a dictionary with weather records for each city.

c) restructure_geographical_data:
- Extracts relevant fields from the geographical records.
- Returns a dictionary with geographical fields and longitude/latitude data.

d) process_geographical_records:
- Extracts the weather fields (city, country, state) from the geographical_fields_dict.

e) get_longitude_latitude:
- Extracts the latitude and longitude data from the geographical_fields_dict.

f) merge_current_weather_data:
- Combines the current weather data from the API with the previously retrieved country and state information.
- Returns a list of dictionaries with complete current weather information for each city.

g) get_merged_weather_records:
- Extracts the merged weather records from the merge_weather_data task output.

h) transform_weather_records:
- Transforms the weather records into a more structured format.
- Converts timestamps to datetime objects and selects specific fields.

i) load_records_to_location_dim:
-  loads the transformed weather records into the Location dimension table in the postgres database.
- It also checks if the records already exist in the weather fact table.
- If the records do not exist in the fact table, it inserts the records into the fact table.

j) create_date_dim:
- Loads the transformed weather records into the Date Dimension table in the postgres database.
- It also checks if the records already exist in the date dimension table.
- If the records do not exist in the date dimension table, it inserts the records into the date dimension

k) join_date_dim_with_weather_fact:
- Joins the Date Dimension table with the Weather Fact table in the postgres database.
- It checks if the records already exist in the date dimension table and the weather fact table.
- If the records exist in both tables, it joins the records by updating the date_id field in the weather fact table.

l) load_records_to_database:
- Loads the transformed weather records into a Postgres database.
- Checks for existing records to avoid duplicates.


3. DAG Structure:

The DAG is structured as follows:

```python

@dag(
    start_date=datetime(2024, 9, 24),
    schedule_interval=timedelta(hours=1),
    description="Weather ETL DAG that fetches weather data from the OpenWeather API, transforms the data and loads it into a Postgres database",
    catchup=False,
    tags=["weather"],
    max_active_runs=1,
    render_template_as_native_obj=True,
)
def weather_etl_dag():
    """
    This function is used to create a Directed Acyclic Graph (DAG) for the Weather ETL process
    The DAG is used to fetch weather data from the OpenWeather API, transform the data and load it into a Postgres database
    """

    get_country_codes = get_country_code()["country_codes"]
    get_weather_info = get_geographical_data(
        get_country_codes, AIRFLOW_CITY_NAMES, AIRFLOW_FIELDS
    )["weather_records"]
    weather_fields_dict = restructure_geographical_data(get_weather_info)[
        "weather_fields"
    ]
    weather_fields_records = process_geographical_records(weather_fields_dict)
    long_lat = get_longitude_latitude(weather_fields_dict)
    merging_weather_data = merge_current_weather_data(
        long_lat,
        AIRFLOW_WEATHER_FIELDS_EXCLUDE,
        weather_fields_records,
        AIRFLOW_API_KEY,
    )

    merged_weather_records = get_merged_weather_records(merging_weather_data)
    transform_records = transform_weather_records(merged_weather_records)

    # task dependencies
    (
        load_records_to_location_dim(
            transform_records, LocationDim, WeatherFact, gen_hash_key_location_dim
        )
        >> load_records_to_weather_type_dim(
            transform_records,
            WeatherTypeDim,
            WeatherFact,
            gen_hash_key_weather_type_dim,
        )
        >> create_date_dim(
            AIRFLOW_START_DATE_YEAR,
            AIRFLOW_END_DATE_YEAR,
            DateDim,
            gen_hash_key_datedim,
        )
        >> join_date_dim_with_weather_fact(WeatherFact, DateDim, json_str="")
    )


weather_dag_instance = weather_etl_dag()

```

4. How the DAG works:

1. The DAG starts by getting country codes for the specified countries.
2. It then fetches geographical information for the specified cities using these country codes.
3. The geographical fields are extracted and separated into two parts: geographical information (city, country, state) and longitude/latitude data.
4. The current weather data from the API is merged with the country and state information.
5. The merged current weather records are then transformed into a more structured format.
6. It then loads the transformed weather records into the Location dimension table in the postgres database. It also checks if the records already exist in the weather fact table. If the records do not exist in the fact table, it inserts the records into the fact table.
7. It loads the transformed weather records into the Date Dimension table in the postgres database. It also checks if the records already exist in the date dimension table. If the records do not exist in the date dimension table, it inserts the records into the date dimension.
8. It now joins the Date Dimension table with the Weather Fact table in the postgres database. It checks if the records already exist in the date dimension table and the weather fact table. If the records exist in both tables, it joins the records by updating the date_id field in the weather fact table.
9. It then loads the transformed weather records into a Postgres database and checks for existing records to avoid duplicates.

Configuration
The DAG uses several configuration variables:

1. AIRFLOW_COUNTRY_NAMES: List of country names to fetch weather data for.
2. AIRFLOW_CITY_NAMES: List of city names to fetch weather data for.
3. AIRFLOW_FIELDS: List of geographical fields to retrieve from the API.
4. AIRFLOW_WEATHER_FIELDS_EXCLUDE: Weather fields to exclude from the API response.
5. AIRFLOW_API_KEY: OpenWeather API key.
6. AIRFLOW_START_DATE_YEAR: The year to start creating date records from
7. AIRFLOW_END_DATE_YEAR: The year to stop creating date records

Each task is dependent on the output of the previous task, creating a linear workflow for the ETL process. The DAG is scheduled to run every hour, ensuring that the database is regularly updated with the latest weather information for the specified cities.

Airflow Variables
![Airflow Variables](images/dag_variables.jpg)


Dag Workflow
![Airflow DAG Success](images/dag_dag.jpg)
