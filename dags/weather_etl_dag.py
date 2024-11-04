import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from config.config import (
    AIRFLOW_API_KEY,
    AIRFLOW_CITY_NAMES,
    AIRFLOW_COUNTRY_NAMES,
    AIRFLOW_FIELDS,
    AIRFLOW_WEATHER_FIELDS_EXCLUDE,
    AIRFLOW_COUNTRY_CITY_API_KEY,
    BIGQUERY_DATASET_ID,
    GCP_PROJECT_ID,
    FACT_TABLE_NAME,
    DATE_TABLE_NAME,
    LOCATION_TABLE_NAME,
    WEATHER_TYPE_TABLE_NAME,
)

from helpers.schema import (
    weather_fact_schema,
    location_dim_schema,
    weather_type_dim_schema,
)

from helpers.utils import (
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
    join_date_dim_with_weather_fact,
)


dataset_id = create_dataset(GCP_PROJECT_ID, BIGQUERY_DATASET_ID)["dataset_id"]

country_codes = get_country_code(AIRFLOW_COUNTRY_NAMES)["country_codes"]
weather_fields = get_weather_fields(
    country_codes,
    AIRFLOW_CITY_NAMES,
    AIRFLOW_FIELDS,
    AIRFLOW_API_KEY,
    AIRFLOW_COUNTRY_CITY_API_KEY,
)["weather_fields"]

structured_weather_fields = restructure_geographical_data(weather_fields)[
    "weather_fields"
]

lon_lat = structured_weather_fields["lon_lat"]
merged_weather_data = merge_current_weather_data(
    lon_lat,
    AIRFLOW_WEATHER_FIELDS_EXCLUDE,
    structured_weather_fields["weather_fields"],
    AIRFLOW_API_KEY,
)["weather_records"]

transformed_records = transform_weather_records(merged_weather_data)["weather_records"]

load_location_dim_records = load_records_to_location_dim(
    transformed_records,
    dataset_id,
    LOCATION_TABLE_NAME,
    FACT_TABLE_NAME,
    gen_hash_key_location_dim,
    gen_hash_key_weatherfact,
    location_dim_schema,
    weather_fact_schema,
)


fact_records = load_location_dim_records["weather_records"]


load_weather_type_records = load_records_to_weather_type_dim(
    transformed_records,
    dataset_id,
    fact_records,
    FACT_TABLE_NAME,
    WEATHER_TYPE_TABLE_NAME,
    gen_hash_key_weather_type_dim,
    weather_type_dim_schema,
    weather_fact_schema,
)

fact_records = load_weather_type_records["weather_records"]

# fact_records = [{'id': '6ce8bbb7d4538d3742a3bed49c5833b0309820a298d8ac6c8cab649a911439d4', 'location_id': '86853afb3a934092311d747e439c2fd9347bd83694dcbb4ffd99fbbf5b8d85b7', 'temperature': 306.2, 'feels_like': 308.71, 'pressure': 1010, 'humidity': 47, 'dew_point': 293.38, 'ultraviolet_index': 8.69, 'clouds': 29, 'visibility': 10000, 'wind_speed': 0.68, 'wind_deg': 21, 'sunrise': 1730611951, 'sunset': 1730654600, 'date': '2024-11-03', 'created_at': '2024-11-03 14:21:36', 'updated_at': '2024-11-03 14:21:42', 'weather_type_id': '15fa97b5a488542761982797f17fd16206a9c1481fb141db5cfe08a38b8f1243'}, {'id': 'fae4d9ba69cbd7dad832ae820b97e96a6804ce654e96d8ce2db8999a09d20bb4', 'location_id': '725fb34e4519be94729b639870b0ebb42cba3e23b570a3aa58e44fb87ccc5000', 'temperature': 304.85, 'feels_like': 302.66, 'pressure': 1012, 'humidity': 13, 'dew_point': 273.09, 'ultraviolet_index': 6.57, 'clouds': 100, 'visibility': 10000, 'wind_speed': 5.4, 'wind_deg': 62, 'sunrise': 1730611146, 'sunset': 1730653182, 'date': '2024-11-03', 'created_at': '2024-11-03 14:21:36', 'updated_at': '2024-11-03 14:21:42', 'weather_type_id': '15fa97b5a488542761982797f17fd16206a9c1481fb141db5cfe08a38b8f1243'}, {'id': '12bd3dce10332c2e9a2dbb7de660f2f054b0d09f0d72aad7d4b0c0d2dec5150b', 'location_id': '2848ea604f745e91c8c44ab3a629cbb71cd2dae371340d4872dcc79863710cec', 'temperature': 306.23, 'feels_like': 305.55, 'pressure': 1011, 'humidity': 32, 'dew_point': 287.34, 'ultraviolet_index': 7.96, 'clouds': 15, 'visibility': 10000, 'wind_speed': 1.09, 'wind_deg': 112, 'sunrise': 1730611200, 'sunset': 1730653628, 'date': '2024-11-03', 'created_at': '2024-11-03 14:21:36', 'updated_at': '2024-11-03 14:21:42', 'weather_type_id': '15fa97b5a488542761982797f17fd16206a9c1481fb141db5cfe08a38b8f1243'}, {'id': '14fba0747a0ed26273549bbcb31c2734d5dadfb7d1a750f09b58e8c60b7eaa0c', 'location_id': '9db9d07b09fad07bda137d527929c0ce615d5414b8a655653951c79a192ca7a5', 'temperature': 306.29, 'feels_like': 310.02, 'pressure': 1009, 'humidity': 51, 'dew_point': 294.79, 'ultraviolet_index': 7.57, 'clouds': 94, 'visibility': 10000, 'wind_speed': 2.53, 'wind_deg': 138, 'sunrise': 1730611031, 'sunset': 1730654022, 'date': '2024-11-03', 'created_at': '2024-11-03 14:21:36', 'updated_at': '2024-11-03 14:21:42', 'weather_type_id': '319b44c570a417ff3444896cd4aa77f052b6781773fc2f9aa1f1180ac745005c'}, {'id': '2adf3226fe9f8e898d605ff896c2d983bc1c3fd5c36a46a5e295309196203315', 'location_id': '6e40814c012c434fc444e640dd634798ac200668447f8c893dfa1cd68dffe625', 'temperature': 304.34, 'feels_like': 310.05, 'pressure': 1010, 'humidity': 66, 'dew_point': 297.23, 'ultraviolet_index': 9.67, 'clouds': 40, 'visibility': 10000, 'wind_speed': 4.63, 'wind_deg': 160, 'sunrise': 1730612816, 'sunset': 1730655703, 'date': '2024-11-03', 'created_at': '2024-11-03 14:21:36', 'updated_at': '2024-11-03 14:21:42', 'weather_type_id': '15fa97b5a488542761982797f17fd16206a9c1481fb141db5cfe08a38b8f1243'}, {'id': 'dea57170e4d75421f87ac519938a40fa5cfc45f14dd980e80be133036d2760ba', 'location_id': 'f93f48c4d29de352d3121bb76019a69f122c0f637755596dfa4690f84516f671', 'temperature': 307.86, 'feels_like': 305.99, 'pressure': 1011, 'humidity': 21, 'dew_point': 282.34, 'ultraviolet_index': 5.95, 'clouds': 19, 'visibility': 10000, 'wind_speed': 4.63, 'wind_deg': 360, 'sunrise': 1730603811, 'sunset': 1730651081, 'date': '2024-11-03', 'created_at': '2024-11-03 14:21:36', 'updated_at': '2024-11-03 14:21:42', 'weather_type_id': '15fa97b5a488542761982797f17fd16206a9c1481fb141db5cfe08a38b8f1243'}, {'id': 'e13c2fabeb01c5fdfd3ab17959da6bb79362a7cfd4e23d7eebe5cd053a238644', 'location_id': '69a99e359a8c9010f419b53d3fba5c888c094235a062a3b7ef5e0dedae6ceae3', 'temperature': 284.18, 'feels_like': 283.01, 'pressure': 1023, 'humidity': 64, 'dew_point': 277.64, 'ultraviolet_index': 0, 'clouds': 100, 'visibility': 10000, 'wind_speed': 6.26, 'wind_deg': 118, 'sunrise': 1730636756, 'sunset': 1730673726, 'date': '2024-11-03', 'created_at': '2024-11-03 14:21:36', 'updated_at': '2024-11-03 14:21:42', 'weather_type_id': '15fa97b5a488542761982797f17fd16206a9c1481fb141db5cfe08a38b8f1243'}, {'id': '894ce912cb8fcd851eaf3a90da507f50aa1ee2b6d52504fe8d479d5f8aaa619c', 'location_id': 'df69a5051a6d522ba654078c7000db25ba6617df2740c45596e387483678c95b',
# 'temperature': 286.79, 'feels_like': 285.93, 'pressure': 1010, 'humidity': 66, 'dew_point': 280.56, 'ultraviolet_index': 0, 'clouds': 0, 'visibility': 10000, 'wind_speed': 2.57, 'wind_deg': 0, 'sunrise': 1730643292, 'sunset': 1730681887, 'date': '2024-11-03', 'created_at': '2024-11-03 14:21:36', 'updated_at': '2024-11-03 14:21:42', 'weather_type_id': '913a4cb91be20332f3559f8070255d7ac3e6228bb423f4441551d3f783e7d4f4'}]

load_date_dim_records = join_date_dim_with_weather_fact(
    FACT_TABLE_NAME, DATE_TABLE_NAME, dataset_id, fact_records
)

fact_records = load_date_dim_records["weather_records"]


print("fact_records", fact_records)
print("len(fact_records)", len(fact_records))


# def retrieve_country_codes():
#     result = get_country_code(AIRFLOW_COUNTRY_NAMES)
#     return result["country_codes"]


# def retrieve_weather_fields(ti):
#     country_codes = ti.xcom_pull(task_ids="get_country_codes")
#     weather_fields = get_weather_fields(
#         country_codes,
#         AIRFLOW_CITY_NAMES,
#         AIRFLOW_FIELDS,
#         AIRFLOW_API_KEY,
#         AIRFLOW_COUNTRY_CITY_API_KEY,
#     )["weather_fields"]
#     return weather_fields


# def restructure_weather_fields(ti):
#     weather_fields = ti.xcom_pull(task_ids="retrieve_weather_fields")
#     structured_weather_fields = restructure_geographical_data(weather_fields)[
#         "weather_fields"
#     ]
#     return structured_weather_fields


# def merge_weather_data(ti):
#     weather_fields_dict = ti.xcom_pull(task_ids="restructure_weather_fields")
#     lon_lat = weather_fields_dict["lon_lat"]
#     merged_weather_data = merge_current_weather_data(
#         lon_lat,
#         AIRFLOW_WEATHER_FIELDS_EXCLUDE,
#         weather_fields_dict["weather_fields"],
#         AIRFLOW_API_KEY,
#     )["weather_records"]
#     return merged_weather_data


# def transform_records(ti):
#     merged_weather_data = ti.xcom_pull(task_ids="merge_weather_data")
#     transform_records = transform_weather_records(merged_weather_data)[
#         "weather_records"
#     ]
#     return transform_records


# def load_location_dim(ti):
#     transformed_records = ti.xcom_pull(task_ids="transform_records")
#     dataset_id = ti.xcom_pull(task_ids="create_bigquery_dataset")
#     load_location_dim_records = load_records_to_location_dim(
#         transformed_records,
#         dataset_id,
#         LOCATION_TABLE_NAME,
#         FACT_TABLE_NAME,
#         gen_hash_key_location_dim,
#         gen_hash_key_weatherfact,
#         location_dim_schema,
#         weather_fact_schema
#     )
#     return load_location_dim_records['weather_records']


# def load_weather_type_dim(ti):
#     transformed_records = ti.xcom_pull(task_ids="transform_records")
#     fact_records = ti.xcom_pull(task_ids="load_location_dim")
#     dataset_id = ti.xcom_pull(task_ids="create_bigquery_dataset")
#     load_weather_type_records = load_records_to_weather_type_dim(
#         transformed_records,
#         dataset_id,
#         fact_records,
#         FACT_TABLE_NAME,
#         WEATHER_TYPE_TABLE_NAME,
#         gen_hash_key_weather_type_dim,
#         weather_type_dim_schema,
#         weather_fact_schema
#     )
#     return load_weather_type_records


# def load_date_dim(ti):
#     dataset_id = ti.xcom_pull(task_ids="create_bigquery_dataset")
#     load_date_dim_records = create_date_dim(
#         AIRFLOW_START_DATE_YEAR,
#         AIRFLOW_END_DATE_YEAR,
#         DATE_TABLE_NAME,
#         date_dim_schema,
#         dataset_id,
#         gen_hash_key_datedim,
#     )
#     return load_date_dim_records


# def join_date_dim_and_fact(ti):
#     dataset_id = ti.xcom_pull(task_ids="create_bigquery_dataset")
#     fact_records = ti.xcom_pull(task_ids="load_weather_type_dim")
#     job_result = join_date_dim_with_weather_fact(
#         FACT_TABLE_NAME, DATE_TABLE_NAME, dataset_id,
#         fact_records
#     )
#     return job_result


# with DAG(
#     dag_id="weather_etl_dag_hourly",
#     start_date=datetime(2024, 10, 29),
#     schedule_interval=timedelta(hours=1),
#     description="Weather ETL DAG that fetches weather data from the OpenWeather API, transforms the data and loads it into BigQuery",
#     catchup=False,
#     tags=["weather"],
#     max_active_runs=1,
#     render_template_as_native_obj=True,
# ) as dag:
#     create_dataset_task = PythonOperator(
#         task_id="create_bigquery_dataset",
#         python_callable=create_bigquery_dataset,
#     )

#     get_country_codes_task = PythonOperator(
#         task_id="get_country_codes",
#         python_callable=retrieve_country_codes,
#     )

#     retrieve_weather_fields_task = PythonOperator(
#         task_id="retrieve_weather_fields",
#         python_callable=retrieve_weather_fields,
#     )

#     restructure_weather_fields_task = PythonOperator(
#         task_id="restructure_weather_fields",
#         python_callable=restructure_weather_fields,
#     )

#     merge_weather_data_task = PythonOperator(
#         task_id="merge_weather_data",
#         python_callable=merge_weather_data,
#     )

#     transform_records_task = PythonOperator(
#         task_id="transform_records",
#         python_callable=transform_records,
#     )

#     load_location_dim_task = PythonOperator(
#         task_id="load_location_dim",
#         python_callable=load_location_dim,
#     )

#     load_weather_type_dim_task = PythonOperator(
#         task_id="load_weather_type_dim",
#         python_callable=load_weather_type_dim,
#     )

#     load_date_dim_task = PythonOperator(
#         task_id="load_date_dim",
#         python_callable=load_date_dim,
#     )

#     join_date_dim_with_fact_task = PythonOperator(
#         task_id="join_date_dim_with_fact",
#         python_callable=join_date_dim_and_fact,
#     )

#     (
#         create_dataset_task
#         >> get_country_codes_task
#         >> retrieve_weather_fields_task
#         >> restructure_weather_fields_task
#         >> merge_weather_data_task
#         >> transform_records_task
#         >> load_location_dim_task
#         >> load_weather_type_dim_task
#         >> load_date_dim_task
#         >> join_date_dim_with_fact_task
#     )
