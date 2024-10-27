import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datetime import datetime
from typing import Dict, List, Union, Any

import pandas as pd
import requests
from google.cloud import bigquery

from config.config import (
    AIRFLOW_COUNTRY_NAMES,
    error_logger,
    logger,
)

from helpers.schema import (
    location_dim_schema,
    weather_fact_schema,
    weather_type_dim_schema,
)
from helpers.utils import (
    insert_or_update_records_to_fact_table,
    update_table_existing_record,
    gen_hash_key_location_dim,
    validate_city,
    retrieve_country_code,
    query_bigquery_existing_data,
    insert_new_records,
    create_table,
    update_records,
)


def get_country_code(
    countries: Union[List[str], str] = AIRFLOW_COUNTRY_NAMES,
) -> Dict[str, Union[str, List[str]]]:
    """
    This function is used to get the country code of the country(s) specified

    Args:
        countries (Union[List[str], str]): List of country names to get the country code for or a single country name
    Returns:
        Dict[str, Union[str, List[str]]]: Dictionary containing status, message, and country code(s)

    Example: {
        "status": "success",
        "message": "Country codes for Nigeria are NG",
        "country_codes": "NG"
    }
    """
    try:
        country_code_list = []
        if isinstance(countries, str):
            country = countries.capitalize()
            country_code = retrieve_country_code(country)["country_codes"]
            country_code_list.append(country_code)

        elif isinstance(countries, list):
            for country_name in countries:
                country_name = country_name.capitalize()
                country_code = retrieve_country_code(country_name)["country_codes"]
                country_code_list.append(country_code)

        logger.info(
            {
                "status": "success",
                "message": f"Country code(s) for {countries} are {country_code_list}",
                "country_codes": country_code_list,
            }
        )
        return {
            "status": "success",
            "message": f"Country code(s) for {countries} are {country_code_list}",
            "country_codes": country_code_list,
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": f"Unable to get country code(s) for {countries} from the API",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": f"Unable to get country code(s) for {countries} from the API",
            "error": str(e),
        }


def get_weather_fields(
    country_codes: List[str],
    cities: List[str],
    fields: list,
    api_key: str,
    country_city_api_key: str,
) -> Dict[str, Union[str, List[Dict[str, str]]]]:
    """
    Function to retrieve weather fields from the OpenWeatherMap API using the country code
    and validated city name that will be validated from the country city API

    Args:
    country_code (str): The country code
    cities (List[str]): A list of cities
    fields (list): A list of fields to retrieve from the API
    api_key (str): The API key for the OpenWeatherMap API
    country_city_api_key (str): The API key for the country city API

    Returns:
    Dict[str, Union[str, Dict[str, str]]]: A dictionary containing the status of the operation,
                                            a message and the weather data

    Example:
    >>> get_data_from_country_code({"NG": ["Lagos"]}, ["lat", "lon", "name"], "your_api_key")

    {"status": "success",
    "message": "Weather information for Lagos retrieved successfully",
    "weather_fields": {"lat": "6.5244", "lon": "3.3792", "name": "Lagos"}}

    """

    try:
        if not isinstance(fields, list):
            raise ValueError("Invalid fields. Fields argument must be a list")
        if not isinstance(api_key, str):
            raise ValueError("Invalid API key. API key argument must be a string")
        if not isinstance(cities, list):
            raise ValueError("Invalid cities. Cities argument must be a list")
        if not isinstance(country_codes, list):
            raise ValueError("Invalid countries. Countries argument must be a list")

        validated_country_cities = validate_city(
            country_codes, cities, country_city_api_key
        )["validated_country_cities"]

        print("validated_country_cities", validated_country_cities)

        weather_fields_list = []
        retrieved_city_names = []
        for country_code, cities in validated_country_cities.items():
            for city_name in cities:
                weather_dict = {}
                url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{country_code}&limit=1&appid={api_key}"
                response = requests.get(url)

                data = response.json()
                if data:
                    data = data[0]
                    for field in fields:
                        weather_dict[field] = data.get(field)
                        weather_fields_list.append(weather_dict)
                        retrieved_city_names.append(city_name)

        logger.info(
            {
                "status": "success",
                "message": f"Weather information for {retrieved_city_names} retrieved successfully",
                "weather_fields": weather_fields_list,
            }
        )
        return {
            "status": "success",
            "message": f"Weather information for {retrieved_city_names} retrieved successfully",
            "weather_fields": weather_fields_list,
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": f"Unable to get weather information for {cities} from the API",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": f"Unable to get weather information for {cities} from the API",
            "error": str(e),
        }


def restructure_geographical_data(
    weather_records: List[dict],
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to retrieve the fields of the weather records;
    longitude and latitude of the cities
    city name, country and state of the cities

    Args:
        weather_records(List[dict]): List of dictionaries containing the weather records; city name, country, state
    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the fields of the weather records such as;
          {city name, country, state}, {longitude, latitude} of each city

    Example: {
        "status": "success",
        "message": "Fields of the weather records have been retrieved",
        "weather_fields": {
            "weather_fields": [
                {'city': 'Lagos', 'country': 'Nigeria', 'state': 'Lagos'},
                {'city': 'Ibadan', 'country': 'Nigeria', 'state': 'Oyo'},
                {'city': 'Kano', 'country': 'Nigeria', 'state': 'Kano'},
                {'city': 'Accra', 'country': 'Ghana', 'state': 'Greater Accra'}
            ],
            "lon_lat": [(3.39, 6.46), (3.93, 7.38), (8.52, 11.96), (-0.21, 5.56)]
        }
    }
    """
    if isinstance(weather_records, List):
        for record in weather_records:
            if list(record.keys()) != ["name", "lat", "lon", "country", "state"]:
                error_logger.error(
                    {
                        "status": "error",
                        "message": "Invalid keys in the weather records. Expected keys are ['name', 'lat', 'lon', 'country', 'state']",
                        "error": "Invalid keys",
                    }
                )
                return {
                    "status": "error",
                    "message": "Invalid keys in the weather records. Expected keys are ['name', 'lat', 'lon', 'country', 'state']",
                    "error": f"Invalid keys: {list(record.keys())}",
                }

        lon_lat = [
            (round(record["lon"], 2), round(record["lat"], 2))
            for record in weather_records
        ]

        weather_fields = []
        for record in weather_records:
            fields_dict = {}
            fields_dict["city"] = record["name"]
            fields_dict["country"] = record["country"]
            fields_dict["state"] = record["state"]
            weather_fields.append(fields_dict)

        weather_fields_dict = {"weather_fields": weather_fields, "lon_lat": lon_lat}

        logger.info(
            {
                "status": "success",
                "message": "Fields of the weather records have been retrieved",
                "weather_fields": weather_fields_dict,
            }
        )
        return {
            "status": "success",
            "message": "Fields of the weather records have been retrieved",
            "weather_fields": weather_fields_dict,
        }

    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
            "error": "Invalid input type",
        }


def merge_current_weather_data(
    lon_lat: List[tuple], excluded_fields: str, weather_fields: List[dict], api_key: str
) -> List[dict]:
    """
    This function is used to get the current weather of cities in a country
    by using the longitude and latitude of the cities in an API call.
    It also joins the weather information from the API with the country
    and state information retrieved earlier

    Args:
        lon_lat(List[tuple]): List of tuples containing the longitude and latitude of the cities
        e.g. [(3.39, 6.46), (3.93, 7.38), (8.52, 11.96), (-0.21, 5.56)]
        excluded_fields(str): String containing the fields to be excluded from the API call e.g. 'minutely,hourly,daily'
        weather_fields(List[dict]): List of dictionaries containing names of country, state about the cities
    Returns:
        weather(dict): Dictionary containing the weather information,
        for the specified cities, fields and country code

    Example: {
        "status": "success",
        "message": "Current weather information for {lon_lat} has been retrieved from the API",
        "weather_records" : [ {'lat': 6.46, 'lon': 3.39,
                            'timezone': 'Africa/Lagos',
                            'timezone_offset': 3600,
                             'current': {'dt': 1726747705, 'sunrise': 1726724175
                            '......': '......'}
                            'city': 'Lagos',
                            'country': 'Nigeria',
                            'state': 'Lagos'
                            }
        ]

    """
    if isinstance(lon_lat, List) and isinstance(excluded_fields, str):
        response_list = []
        record_counter = 0
        for lon, lat in lon_lat:
            try:
                url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={excluded_fields}&appid={api_key}"
                response = requests.get(url)
                data = response.json()

                weather_dict = {}
                for key in data.keys():
                    weather_dict[key] = data[key]

                print("data_keys", data.keys())

                weather_dict["city"] = weather_fields[record_counter]["city"]
                weather_dict["country"] = weather_fields[record_counter]["country"]
                weather_dict["state"] = weather_fields[record_counter]["state"]
                record_counter += 1

                response_list.append(weather_dict)

                print("weather_keys", weather_dict.keys())

            except Exception as e:
                error_logger.error(
                    {
                        "status": "error",
                        "message": f"Unable to get weather information for {lon_lat} from the API",
                        "error": str(e),
                    }
                )
                return {
                    "status": "error",
                    "message": f"Unable to get weather information for {lon_lat} from the API",
                    "error": str(e),
                }

        logger.info(
            {
                "status": "success",
                "message": f"Current weather information for {lon_lat} has been retrieved from the API",
                "weather_records": response_list,
            }
        )
        return {
            "status": "success",
            "message": f"Current weather information for {lon_lat} has been retrieved from the API",
            "weather_records": response_list,
        }
    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for lon_lat. Expected list but got {type(lon_lat)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for lon_lat. Expected list but got {type(lon_lat)}",
            "error": "Invalid input type",
        }


def transform_weather_records(
    weather_records: List[dict],
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to transform the weather records into a
    a more structured list of dictionaries, with only the necessary fields.
    Each dictionary in the list contains the weather information for each city specified in the API call.
    The datetime field is also converted from a unix timestamp to a datetime object.

    Args:
        weather_records(List[dict]): List of dictionaries containing the weather records
    Returns:
        List[dict]: List of dictionaries containing the transformed weather records
    """

    if isinstance(weather_records, List):
        try:
            transformed_weather_records = []
            for record in weather_records:
                transformed_record = {}
                transformed_record["city"] = record["city"]
                transformed_record["country"] = record["country"]
                transformed_record["state"] = record["state"]
                transformed_record["latitude"] = record["lat"]
                transformed_record["longitude"] = record["lon"]
                transformed_record["timezone"] = record["timezone"]
                transformed_record["timezone_offset"] = record["timezone_offset"]
                date_time = record["current"]["dt"]
                date_time = datetime.fromtimestamp(date_time).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                date_time = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
                transformed_record["date_time"] = date_time
                transformed_record["date"] = date_time.date()
                transformed_record["year"] = transformed_record["date_time"].year
                transformed_record["month"] = transformed_record["date_time"].month
                transformed_record["day"] = transformed_record["date_time"].day
                transformed_record["day_of_week"] = transformed_record[
                    "date_time"
                ].strftime("%A")
                transformed_record["sunrise"] = record["current"]["sunrise"]
                transformed_record["sunset"] = record["current"]["sunset"]
                transformed_record["temp"] = record["current"]["temp"]
                transformed_record["feels_like"] = record["current"]["feels_like"]
                transformed_record["pressure"] = record["current"]["pressure"]
                transformed_record["humidity"] = record["current"]["humidity"]
                transformed_record["dew_point"] = record["current"]["dew_point"]
                transformed_record["ultraviolet_index"] = record["current"]["uvi"]
                transformed_record["clouds"] = record["current"]["clouds"]
                transformed_record["visibility"] = record["current"]["visibility"]
                transformed_record["wind_speed"] = record["current"]["wind_speed"]
                transformed_record["wind_deg"] = record["current"]["wind_deg"]
                transformed_record["weather"] = record["current"]["weather"][0]["main"]
                transformed_record["description"] = record["current"]["weather"][0][
                    "description"
                ]
                transformed_weather_records.append(transformed_record)

            logger.info(
                {
                    "status": "success",
                    "message": "Weather records have been transformed",
                    "weather_records": transformed_weather_records[:1],
                }
            )
            return {
                "status": "success",
                "message": "Weather records have been transformed",
                "weather_records": transformed_weather_records,
            }

        except Exception as e:
            error_logger.error(
                {
                    "status": "error",
                    "message": "Unable to transform weather records",
                    "error": str(e),
                }
            )
            return {
                "status": "error",
                "message": "Unable to transform weather records",
                "error": str(e),
            }

    else:
        error_logger.error(
            {
                "status": "error",
                "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
                "error": "Invalid input type",
            }
        )
        return {
            "status": "error",
            "message": f"Invalid input type for weather records. Expected list but got {type(weather_records)}",
            "error": "Invalid input type",
        }


def load_records_to_location_dim(
    weather_data: List[Dict[str, Any]],
    dataset_id: str,
    location_table_name: str,
    fact_table_name: str,
    location_hash_function: Dict[str, str],
    fact_hash_function: Dict[str, str],
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to load the transformed weather records into the Location
    Dimension table into BigQuery. It also checks if the records already exist in the weather fact table,
    If the records do not exist in the fact table, it inserts the records into the fact table.

    Args:
        weather_data (List[dict]): List of dictionaries containing the weather records
        dataset_id (str): The dataset ID of the BigQuery dataset
        location_table_name (str): The name of the location dimension table
        fact_table_name (str): The name of the fact table
        location_hash_function (Dict[str, str]): The hash function to generate the hash key for the location dimension table
        fact_hash_function (Dict[str, str]): The hash function to generate the hash key for the fact table
    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the status, message and weather records

    Examples:
    >>> load_records_to_location_dim(weather_data, 'your_project.your_dataset',
                'location_dim', 'weather_fact', location_hash_function, fact_hash_function)
    Returns:
    {
        "status": "success",
        "message": f"{no_data} location records have been loaded to the location table and corresponding records have been updated to the fact table",
        "weather_records": new_records[:5],
    }
    """

    try:
        create_location_table = create_table(
            dataset_id, location_table_name, location_dim_schema
        )
        create_fact_table = create_table(
            dataset_id, fact_table_name, weather_fact_schema
        )

        if create_location_table["status"] != "success":
            return {
                "status": "error",
                "message": "Unable to create location dimension table",
                "error": create_location_table["error"],
            }
        elif create_fact_table["status"] != "success":
            return {
                "status": "error",
                "message": "Unable to create fact table",
                "error": create_fact_table["error"],
            }

        location_table_id = create_location_table["table_id"]
        fact_table_id = create_fact_table["table_id"]

        existing_data = query_bigquery_existing_data(
            location_table_id, weather_data, location_hash_function
        )
        print("existing_data", existing_data)

        existing_data = existing_data["body"]

        existing_ids = existing_data["existing_ids"]
        record_list = existing_data["record_list"]

        no_data = 0
        new_records = []
        newly_added_ids = []
        for record in record_list:
            if record["id"] not in existing_ids:
                if record["id"] not in newly_added_ids:
                    new_record = {
                        "id": record["id"],
                        "city": record["city"],
                        "country": record["country"],
                        "state": record["state"],
                        "latitude": record["latitude"],
                        "longitude": record["longitude"],
                        "timezone": record["timezone"],
                        "timezone_offset": record["timezone_offset"],
                        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }

                    insert_location_record = insert_new_records(
                        location_table_id, new_record
                    )

                    if insert_location_record["status"] != "success":
                        return {
                            "status": "error",
                            "message": "Unable to insert location record to the location dimension table",
                            "error": insert_location_record["error"],
                        }

                    newly_added_ids.append(new_record["id"])
                    new_records.append(new_record)
                    no_data += 1

                    fact_column_to_match = "location_id"
                    record_column_to_match = "id"

                    # insert or update the location record in the fact table
                    insert_or_update_records_to_fact = (
                        insert_or_update_records_to_fact_table(
                            location_table_id,
                            fact_table_id,
                            record,
                            fact_column_to_match,
                            record_column_to_match,
                            fact_hash_function,
                        )
                    )
                    if insert_or_update_records_to_fact["status"] != "success":
                        return {
                            "status": "error",
                            "message": "Unable to insert or update location record to the fact table",
                            "error": insert_or_update_records_to_fact["error"],
                        }

            else:
                # update the corresponding location record in the fact table
                fact_column_to_match = "location_id"
                record_column_to_match = "id"

                insert_or_update_records_to_fact = (
                    insert_or_update_records_to_fact_table(
                        location_table_id,
                        fact_table_id,
                        record,
                        fact_column_to_match,
                        record_column_to_match,
                        fact_hash_function,
                    )
                )
                if insert_or_update_records_to_fact["status"] != "success":
                    return {
                        "status": "error",
                        "message": "Unable to insert or update location record to the fact table",
                        "error": insert_or_update_records_to_fact["error"],
                    }

        logger.info(
            {
                "status": "success",
                "message": f"{no_data} location records have been loaded to the location table and corresponding records have been updated to the fact table",
                "weather_records": new_records[:5],
            }
        )
        return {
            "status": "success",
            "message": f"{no_data} location records have been loaded to the location table and corresponding records have been updated to the fact table",
            "weather_records": new_records[:5],
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to load weather location records to the location dimension table",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to load weather location records to the location dimension table",
            "error": str(e),
        }


def load_records_to_weather_type_dim(
    weather_data: List[Dict[str, Any]],
    dataset_id: str,
    fact_table_name: str,
    weather_type_table_name: str,
    weather_type_hash_function: Dict[str, str],
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to load the transformed weather records into the Weather Type
    Dimension table into BigQuery. It also checks if the records already exist in the weather fact table,
    If the records do not exist in the fact table, it inserts the records into the fact table.

    Args:
        weather_data (List[dict]): List of dictionaries containing the weather records
        dataset_id (str): The dataset ID of the BigQuery dataset
        fact_table_name (str): The name of the fact table
        weather_type_table_name (str): The name of the weather type dimension table
        weather_type_hash_function (Dict[str, str]): The hash function to generate the hash key for the weather type dimension table
    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the status, message and weather records

    Examples:
    >>> load_records_to_weather_type_dim(weather_data, 'your_project.your_dataset',
                'weather_fact', 'weather_type_dim', weather_type_hash_function)
    Returns:
    {
        "status": "success",
        "message": f"{no_data} weather type records have been loaded to the weather type table and corresponding records have been updated to the fact table",
        "weather_records": new_records[:5],
    }
    """

    try:
        client = bigquery.Client()
        create_fact_table = create_table(
            dataset_id, fact_table_name, weather_fact_schema
        )
        create_weather_type_table = create_table(
            dataset_id, weather_type_table_name, weather_type_dim_schema
        )

        if create_weather_type_table["status"] != "success":
            return {
                "status": "error",
                "message": "Unable to create location dimension table",
                "error": create_weather_type_table["error"],
            }
        elif create_fact_table["status"] != "success":
            return {
                "status": "error",
                "message": "Unable to create fact table",
                "error": create_fact_table["error"],
            }

        weather_type_table_id = create_weather_type_table["table_id"]

        print("weather_type_table_id", weather_type_table_id)
        fact_table_id = create_fact_table["table_id"]

        print("fact_table_id", fact_table_id)

        weather_type_existing_data = query_bigquery_existing_data(
            weather_type_table_id, weather_data, weather_type_hash_function
        )["body"]
        existing_ids = weather_type_existing_data["existing_ids"]
        record_list = weather_type_existing_data["record_list"]

        no_data = 0
        new_records = []
        newly_added_ids = []
        for record in record_list:
            if record["id"] not in existing_ids:
                if record["id"] not in newly_added_ids:
                    new_record = {
                        "id": record["id"],
                        "weather": record["weather"],
                        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    new_record_location_id = gen_hash_key_location_dim(record)[
                        "hash_key"
                    ]

                    print("new_record", new_record)
                    print("new_record_location_id", new_record_location_id)

                    insert_weather_type_record = insert_new_records(
                        weather_type_table_id, new_record
                    )

                    print("insert_weather_type_record", insert_weather_type_record)
                    newly_added_ids.append(new_record["id"])

                    if insert_weather_type_record["status"] != "success":
                        return {
                            "status": "error",
                            "message": "Unable to insert location record to the weather_type_dim dimension table",
                            "error": insert_weather_type_record["error"],
                        }

                    fact_table_ref = client.get_table(fact_table_id)
                    fact_records = (
                        client.list_rows(fact_table_ref)
                        .to_dataframe()
                        .to_dict(orient="records")
                    )

                    print("fact_records", fact_records)

                    existing_fact_records = [
                        fact_record
                        for fact_record in fact_records
                        if fact_record["weather_type_id"] == new_record["id"]
                    ]

                    print("existing_fact_records", existing_fact_records)

                    # if the weather_type does not exist in the fact_table update the fact_table wih weather_type_id
                    if not existing_fact_records:
                        existing_fact_record = [
                            fact_record
                            for fact_record in fact_records
                            if fact_record["location_id"] == new_record_location_id
                        ]
                        print("existing_fact_recorddddd", existing_fact_record)

                        update_fact_mapper = {"weather_type_id": "id", "date": "date"}

                        update_fact_record = update_table_existing_record(
                            existing_fact_record,
                            fact_table_id,
                            new_record,
                            update_fact_mapper,
                        )

                        print("update_fact_record", update_fact_record)

                        if update_fact_record["status"] != "success":
                            return {
                                "status": "error",
                                "message": "Unable to insert corresponding weather_type_id record to the fact table",
                                "error": update_fact_record["error"],
                            }
                    else:
                        # if the weather_type exist in the fact_table update the fact_table wih the corresponding weather_type_id
                        update_fact_mapper = {"weather_type_id": "id", "date": "date"}

                        update_fact_record = update_table_existing_record(
                            existing_fact_records,
                            fact_table_id,
                            new_record,
                            update_fact_mapper,
                        )
                        print("update_fact_record", update_fact_record)
                        if update_fact_record["status"] != "success":
                            return {
                                "status": "error",
                                "message": "Unable to update corresponding weather_type_id record in the fact table",
                                "error": update_fact_record["error"],
                            }

                    new_records.append(new_record)
                    no_data += 1

            else:
                # update the corresponding weather_type record in the fact table

                fact_table_ref = client.get_table(fact_table_id)
                fact_records = (
                    client.list_rows(fact_table_ref)
                    .to_dataframe()
                    .to_dict(orient="records")
                )
                existing_fact_records = [
                    fact_record
                    for fact_record in fact_records
                    if fact_record["weather_type_id"] == record["id"]
                ]

                update_fact_mapper = {"weather_type_id": "id", "date": "date"}

                update_fact_record = update_table_existing_record(
                    existing_fact_records, fact_table_id, record, update_fact_mapper
                )
                if update_fact_record["status"] != "success":
                    return {
                        "status": "error",
                        "message": "Unable to update corresponding weather_type_id record in the fact table",
                        "error": update_fact_record["error"],
                    }

        logger.info(
            {
                "status": "success",
                "message": f"{no_data} weather_type records have been loaded to the weather_type table and corresponding records have been updated to the fact table",
                "weather_records": new_records[:5],
            }
        )
        return {
            "status": "success",
            "message": f"{no_data} weather_type records have been loaded to the weather_type table and corresponding records have been updated to the fact table",
            "weather_records": new_records[:5],
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to load weather type records to the weather type dimension table",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to load weather type records to the weather type dimension table",
            "error": str(e),
        }


def create_date_dim(
    start_year: str,
    end_year: str,
    date_table_name: str,
    date_dim_schema: List[dict],
    dataset_id: str,
    hash_function: Dict[str, str],
) -> Dict[str, Union[str, List[dict]]]:
    """
    This function is used to load the transformed weather records into the Date Dimension table into the BigQuery.
    It also checks if the records already exist in the date dimension table.
    If the records do not exist in the date dimension table, it inserts the records into the date dimension

    Args:
        start_year (str): The start year of the date range
        end_year (str): The end year of the date range
        date_table_name (str): The name of the date dimension table
        date_dim_schema (List[dict]): The schema of the date dimension table
        dataset_id (str): The dataset ID of the BigQuery dataset
        hash_function (Dict[str, str]): The hash function to generate the hash key for the date dimension table
    Returns:
        Dict[str, Union[str, List[dict]]]: Dictionary containing the status, message and weather records

    Examples:
    >>> create_date_dim('2020', '2021', 'date_dim', date_dim_schema,
                 'your_project.your_dataset', gen_hash_key_datedim)
    Returns:
    {
        "status": "success",
        "message": f"{no_data} date records have been loaded to the date dimension table",
        "date_records": new_records[:5],
    }

    """

    try:
        client = bigquery.Client()
        create_date_table = create_table(dataset_id, date_table_name, date_dim_schema)

        if create_date_table["status"] != "success":
            return {
                "status": "error",
                "message": "Unable to create date dimension table",
                "error": create_date_table["error"],
            }

        date_table_id = create_date_table["table_id"]

        start_date = f"{start_year}-01-01"
        end_date = f"{end_year}-12-31"
        date_range = pd.date_range(start_date, end_date)
        date_records = []
        for date in date_range:
            record = {
                "date": date,
                "year": date.year,
                "month": date.month,
                "day": date.day,
                "day_of_week": date.strftime("%A"),
            }
            date_records.append(record)

        existing_data = query_bigquery_existing_data(
            date_table_id, date_records, hash_function
        )["body"]
        existing_ids = existing_data["existing_ids"]
        record_list = existing_data["record_list"]
        no_data = 0
        new_records = []
        newly_added_ids = []

        for record in record_list:
            if not existing_ids:
                if record["id"] not in existing_ids:
                    if record["id"] not in newly_added_ids:
                        new_record = {
                            "id": record["id"],
                            "date": record["date"].date().isoformat(),
                            "year": record["year"],
                            "month": record["month"],
                            "day": record["day"],
                            "day_of_week": record["day_of_week"],
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        insert_date_record = insert_new_records(
                            date_table_id, new_record
                        )
                        newly_added_ids.append(new_record["id"])

                        if insert_date_record["status"] != "success":
                            return {
                                "status": "error",
                                "message": "Unable to insert date record to the date dimension table",
                                "error": insert_date_record["error"],
                            }

                        new_records.append(new_record)
                        no_data += 1

            else:
                # "updated at" column of subsequent records to be loaded to the date dimension table after the first load should be updated

                if record["id"] not in existing_ids:
                    if record["id"] not in newly_added_ids:
                        date_table_ref = client.get_table(date_table_id)
                        existing_date_records = (
                            client.list_rows(date_table_ref)
                            .to_dataframe()
                            .to_dict(orient="records")
                        )
                        first_existing_date_record = existing_date_records[0]

                        new_record = {
                            "id": record["id"],
                            "date": record["date"].date().isoformat(),
                            "year": record["year"],
                            "month": record["month"],
                            "day": record["day"],
                            "day_of_week": record["day_of_week"],
                            "created_at": first_existing_date_record[
                                "created_at"
                            ].isoformat(),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        insert_date_record = insert_new_records(
                            date_table_id, new_record
                        )
                        newly_added_ids.append(new_record["id"])

                        if insert_date_record["status"] != "success":
                            return {
                                "status": "error",
                                "message": "Unable to insert date record to the date dimension table",
                                "error": insert_date_record["error"],
                            }
                        new_records.append(new_record)
                        no_data += 1

        logger.info(
            {
                "status": "success",
                "message": f"{no_data} date records have been loaded to the date dimension table",
                "date_records": new_records[:5],
            }
        )
        return {
            "status": "success",
            "message": f"{no_data} date records have been loaded to the date dimension table",
            "date_records": new_records[:1],
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to load weather date records to the date dimension table",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to load weather date records to the date dimension table",
            "error": str(e),
        }


def join_date_dim_with_weather_fact(
    fact_table_name: str, date_table_name: str, dataset_id: str
):
    """
    This function is used to join the date records with the weather fact table
    The date records are joined with the weather fact table using the date field in the fact table
    The date field in the fact table is converted to a date id using the date dimension table
    The date id is then used to join the date records with the weather fact table

    Args:
        fact_table_name (str): The name of the weather fact table
        date_table_name (str): The name of the date dimension table
        dataset_id (str): The dataset ID of the BigQuery dataset
    """
    try:
        client = bigquery.Client()
        fact_table_ref = client.dataset(dataset_id).table(fact_table_name)
        get_fact_table_id = client.get_table(fact_table_ref)
        fact_records = (
            client.list_rows(get_fact_table_id).to_dataframe().to_dict(orient="records")
        )

        date_table_ref = client.dataset(dataset_id).table(date_table_name)
        get_date_table_id = client.get_table(date_table_ref)
        date_records = (
            client.list_rows(get_date_table_id).to_dataframe().to_dict(orient="records")
        )

        if fact_records and date_records:
            no_data = 0
            for fact_record in fact_records:
                fact_record_date = fact_record["date"].strftime("%Y-%m-%d")
                list_date_records = [
                    date_record["date"].strftime("%Y-%m-%d")
                    for date_record in date_records
                ]

                if list_date_records.index(fact_record_date):
                    date_record = date_records[
                        list_date_records.index(fact_record_date)
                    ]
                    fact_record["date_id"] = date_record["id"]
                    fact_record["date"] = fact_record["date"].isoformat()
                    fact_record["created_at"] = fact_record["created_at"].isoformat()
                    fact_record["updated_at"] = fact_record["updated_at"].isoformat()
                    update_fact_record = update_records(get_fact_table_id, fact_record)
                    no_data += 1

                    if update_fact_record["status"] != "success":
                        return {
                            "status": "error",
                            "message": "Unable to update weather fact with date id",
                            "error": update_fact_record["error"],
                        }

            logger.info(
                {
                    "status": "success",
                    "message": f"{no_data} Date records have been joined with the weather fact table",
                    "weather_records": fact_records[:1],
                }
            )
            return {
                "status": "success",
                "message": "Date records have been joined with the weather fact table",
                "weather_records": fact_records[:1],
            }

        else:
            error_logger.error(
                {
                    "status": "error",
                    "message": "No records found in the date or weather fact table",
                    "error": "No records found",
                }
            )
            return {
                "status": "error",
                "message": "No records found in the date or weather fact table",
                "error": "No records found",
            }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to join date records with the weather fact table",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to join date records with the weather fact table",
            "error": str(e),
        }
