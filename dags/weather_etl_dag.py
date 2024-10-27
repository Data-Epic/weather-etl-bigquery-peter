import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from config.config import (
    AIRFLOW_API_KEY,
    AIRFLOW_CITY_NAMES,
    AIRFLOW_COUNTRY_NAMES,
    AIRFLOW_END_DATE_YEAR,
    AIRFLOW_FIELDS,
    AIRFLOW_START_DATE_YEAR,
    AIRFLOW_WEATHER_FIELDS_EXCLUDE,
    AIRFLOW_COUNTRY_CITY_API_KEY,
    BIGQUERY_DATASET_ID,
    GCP_PROJECT_ID,
    FACT_TABLE_NAME,
    DATE_TABLE_NAME,
    LOCATION_TABLE_NAME,
    WEATHER_TYPE_TABLE_NAME,
)

from helpers.schema import date_dim_schema

from helpers.utils import (
    gen_hash_key_datedim,
    gen_hash_key_location_dim,
    gen_hash_key_weather_type_dim,
    gen_hash_key_weatherfact,
    create_dataset,
)

from helpers.weather_etl import (
    get_country_code,
    get_weather_fields,
    restructure_geographical_data,
    merge_current_weather_data,
    transform_weather_records,
    load_records_to_location_dim,
    load_records_to_weather_type_dim,
    create_date_dim,
    join_date_dim_with_weather_fact,
)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def retrieve_country_codes():
    result = get_country_code(AIRFLOW_COUNTRY_NAMES)
    return result["country_codes"]


def retrieve_weather_fields():
    country_codes = retrieve_country_codes()
    weather_fields = get_weather_fields(
        country_codes,
        AIRFLOW_CITY_NAMES,
        AIRFLOW_FIELDS,
        AIRFLOW_API_KEY,
        AIRFLOW_COUNTRY_CITY_API_KEY,
    )["weather_fields"]

    return weather_fields


def restructure_weather_fields():
    structured_weather_fields = restructure_geographical_data(
        retrieve_weather_fields()
    )["weather_fields"]
    return structured_weather_fields


def merge_weather_data():
    weather_fields_dict = restructure_weather_fields()
    lon_lat = weather_fields_dict["lon_lat"]
    merged_weather_data = merge_current_weather_data(
        lon_lat,
        AIRFLOW_WEATHER_FIELDS_EXCLUDE,
        weather_fields_dict["weather_fields"],
        AIRFLOW_API_KEY,
    )["weather_records"]
    return merged_weather_data


def transform_records():
    merged_weather_data = merge_weather_data()
    transform_records = transform_weather_records(merged_weather_data)[
        "weather_records"
    ]
    print("transformed records", transform_records)
    return transform_records


transformed_records = transform_records()
create_bq_dataset = create_dataset(GCP_PROJECT_ID, BIGQUERY_DATASET_ID)
dataset_id = create_bq_dataset["dataset_id"]


def load_location_dim():
    load_location_dim_records = load_records_to_location_dim(
        transformed_records,
        dataset_id,
        LOCATION_TABLE_NAME,
        FACT_TABLE_NAME,
        gen_hash_key_location_dim,
        gen_hash_key_weatherfact,
    )

    return load_location_dim_records


def load_weather_type_dim():
    load_weather_type_records = load_records_to_weather_type_dim(
        transformed_records,
        dataset_id,
        FACT_TABLE_NAME,
        WEATHER_TYPE_TABLE_NAME,
        gen_hash_key_weather_type_dim,
    )
    return load_weather_type_records


def load_date_dim():
    load_date_dim_records = create_date_dim(
        AIRFLOW_START_DATE_YEAR,
        AIRFLOW_END_DATE_YEAR,
        DATE_TABLE_NAME,
        date_dim_schema,
        dataset_id,
        gen_hash_key_datedim,
    )
    return load_date_dim_records


def join_date_dim_and_fact():
    job_result = join_date_dim_with_weather_fact(
        FACT_TABLE_NAME, DATE_TABLE_NAME, dataset_id
    )
    return job_result


dag = DAG(
    dag_id="weather_etl_dag_hourly",
    start_date=datetime.datetime(2024, 9, 24),
    schedule_interval=timedelta(hours=1),
    description="Weather ETL DAG that fetches weather data from the OpenWeather API, transforms the data and loads it into a Postgres database",
    catchup=False,
    tags=["weather"],
    max_active_runs=1,
    render_template_as_native_obj=True,
)

get_country_codes_task = PythonOperator(
    task_id="get_country_codes",
    python_callable=retrieve_country_codes,
    dag=dag,
)

retrieve_weather_fields_task = PythonOperator(
    task_id="retrieve_weather_fields",
    python_callable=retrieve_weather_fields,
    dag=dag,
)

restructure_weather_fields_task = PythonOperator(
    task_id="restructure_weather_fields",
    python_callable=restructure_weather_fields,
    dag=dag,
)

merge_weather_data_task = PythonOperator(
    task_id="merge_weather_data",
    python_callable=merge_weather_data,
    dag=dag,
)

transform_records_task = PythonOperator(
    task_id="transform_records",
    python_callable=transform_records,
    dag=dag,
)

load_location_dim_task = PythonOperator(
    task_id="load_location_dim", python_callable=load_location_dim, dag=dag
)

load_weather_type_dim_task = PythonOperator(
    task_id="load_weather_type_dim",
    python_callable=load_weather_type_dim,
    dag=dag,
)

load_date_dim_task = PythonOperator(
    task_id="load_date_dim",
    python_callable=load_date_dim,
    dag=dag,
)

join_date_dim_with_fact_task = PythonOperator(
    task_id="join_date_dim_with_fact",
    python_callable=join_date_dim_and_fact,
    dag=dag,
)

(
    get_country_codes_task
    >> retrieve_weather_fields_task
    >> restructure_weather_fields_task
    >> merge_weather_data_task
    >> transform_records_task
    >> load_location_dim_task
    >> load_weather_type_dim_task
    >> load_date_dim_task
    >> join_date_dim_with_fact_task
)

if __name__ == "__main__":
    dag.test()
