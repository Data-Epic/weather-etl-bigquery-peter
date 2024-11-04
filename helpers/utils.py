import hashlib
import os
import random
import string
import sys
import json
from datetime import datetime
from typing import Dict, List, Union, Any
from google.cloud.exceptions import NotFound, Conflict
from google.cloud.bigquery import Table
import requests
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

from config.config import error_logger, logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def gen_hash_key_weatherfact() -> Dict[str, str]:
    """
    Function to generate a hash key as a primary key for the weather fact table

    Returns:
    Dict[str,str]: A dictionary containing the status of the operation,
                     a message and the hash key

    Example: {
            "status": "success",
            "message": "Hash key generated successfully",
            "hash_key": "c3d8b3d9c4e"
    }
    """
    try:
        random_string = "".join(
            random.choices(string.ascii_letters + string.digits, k=20)
        )
        hash_key = hashlib.sha256(random_string.encode()).hexdigest()

        logger.info(
            {
                "status": "success",
                "message": "Hash key generated successfully",
                "hash_key": "hash_key",
            }
        )

        return {
            "status": "success",
            "message": "Hash key generated successfully",
            "hash_key": hash_key,
        }
    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to generate hash key for weather fact data",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to generate hash key for weather fact data",
            "error": str(e),
        }


def gen_hash_key_location_dim(data: dict) -> Dict[str, str]:
    """
    Function to generate a hash key for the location dimension table

    Args:
    data(dict): A dictionary containing the location data

    Returns:
    Dict[str,str]: A dictionary containing the status of the operation,
                     a message and the hash key

    Example: {
            "status": "success",
            "message": "Hash key generated successfully",
            "hash_key": "c3d8b3d9c4e"
    }
    """

    try:
        if isinstance(data, dict) is True:
            composite_key = f"{data['latitude']}_{data['longitude']}"
            composite_key = composite_key.lower().replace(" ", "")
            hash_object = hashlib.sha256(composite_key.encode())
            hash_key = hash_object.hexdigest()

            logger.info(
                {
                    "status": "success",
                    "message": "Hash key generated successfully",
                    "hash_key": "hash_key",
                }
            )

            return {
                "status": "success",
                "message": "Hash key generated successfully",
                "hash_key": hash_key,
            }

        else:
            return {
                "status": "error",
                "message": "Invalid data format. Data argument of the location data must be a dictionary",
            }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to generate hash key for location data",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to generate hash key for location data",
            "error": str(e),
        }


def gen_hash_key_datedim(data: dict) -> Dict[str, str]:
    """
    Function to generate a hash key for the date data

    Args:
    data (dict): A dictionary containing the date dimension data

    Returns:
    Dict[str,str]: A dictionary containing the status of the operation,
                    a message and the hash key

    Example: {
            "status": "success",
            "message": "Hash key generated successfully",
            "hash_key": "c3d8b3d9
    }

    """
    try:
        if isinstance(data, dict) is True:
            composite_key = f"{data['date']}"
            composite_key = composite_key.lower().replace(" ", "")
            hash_object = hashlib.sha256(composite_key.encode())
            hash_key = hash_object.hexdigest()

            logger.info(
                {
                    "status": "success",
                    "message": "Hash key generated successfully",
                    "hash_key": "hash_key",
                }
            )

            return {
                "status": "success",
                "message": "Hash key generated successfully",
                "hash_key": hash_key,
            }

        else:
            return {
                "status": "error",
                "message": "Invalid data format. Data argument for date must be a dictionary",
            }
    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to generate hash key for date data",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to generate hash key for date data",
            "error": str(e),
        }


def gen_hash_key_weather_type_dim(data: dict) -> Dict[str, str]:
    """
    Function to generate a hash key for the weather type dimension data

    Args:
    data (dict): A dictionary containing the weather data

    Returns:
    Dict[str,str]: A dictionary containing the status of the operation,
                    a message and the hash key

    Example: {
            "status": "success",
            "message": "Hash key generated successfully",
            "hash_key": "c3d8b3d9
    }
    """
    try:
        if isinstance(data, dict) is True:
            composite_key = f"{data['weather']}"
            composite_key = composite_key.lower().replace(" ", "")
            hash_object = hashlib.sha256(composite_key.encode())
            hash_key = hash_object.hexdigest()

            logger.info(
                {
                    "status": "success",
                    "message": "Hash key generated successfully",
                    "hash_key": "hash_key",
                }
            )

            return {
                "status": "success",
                "message": "Hash key generated successfully",
                "hash_key": hash_key,
            }

        else:
            return {
                "status": "error",
                "message": "Invalid data format. Data argument for weather must be a dictionary",
            }
    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to generate hash key for weather data",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to generate hash key for weather data",
            "error": str(e),
        }


def create_dataset(project_id: str, dataset_id: str) -> Dict[str, str]:
    """
    Function to create a BigQuery dataset

    Args:
    project_id (str): The ID of the project
    dataset_id (str): The ID of the dataset

    Returns:
    Dict[str, str]: A dictionary containing the status of the operation

    Example:
    >>> create_dataset("your_project.your_dataset")
    {
    "status": "success",
    "message": "Dataset created successfully",
    "dataset_id": dataset_id
    }
    """

    try:
        client = bigquery.Client()
        dataset_ref = client.dataset(dataset_id, project_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset, timeout=30, exists_ok=False)

        dataset_id = client.get_dataset(dataset)

        logger.info(
            {
                "status": "success",
                "message": "Dataset created successfully",
                "dataset_id": dataset_id,
            }
        )
        return {
            "status": "success",
            "message": "Dataset created successfully",
            "dataset_id": dataset_id,
        }
    except Conflict:
        logger.info(
            {
                "status": "success",
                "message": "Dataset already exists",
                "dataset_id": dataset_id,
            }
        )
        return {
            "status": "success",
            "message": "Dataset already exists",
            "dataset_id": dataset_id,
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "Unable to create dataset",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "Unable to create dataset",
            "error": str(e),
        }


def query_bigquery_existing_data(
    table_id: str,
    data: List[dict],
    hash_function: callable,
) -> Dict[str, Union[str, List]]:
    """
    Function to query existing data from a BigQuery table without using SQL

    Args:
    client (bigquery.Client): A BigQuery client object
    table_id (str): The fully-qualified ID of the table in the format 'project.dataset.table'
    data (List[dict]): A list of dictionaries containing the data to be queried
    hash_function (callable): A function to generate hash keys for the records

    Returns:
    Dict[str, List]: A dictionary containing the existing data

    Example:
    >>> client = bigquery.Client()
    >>> table_id = 'your-project.your_dataset.location_dim'
    >>> data = [{'country': 'Nigeria', 'state': 'Lagos', 'city': 'Ikeja'},
    ...         {'country': 'Nigeria', 'state': 'Lagos', 'city': 'Victoria Island'}]
    >>> result = query_existing_data(client, table_id, data, gen_hash_key_location_dim)
    >>> print(result)
    {
        "existing_data": [{"id": "1", "country": "nigeria", "state": "lagos", "city": "ikeja"},
                          {"id": "2", "country": "nigeria", "state": "lagos", "city": "victoria island"}],
        "existing_ids": ["1", "2"],
        "record_list": [{"country": "nigeria", "state": "lagos", "city": "ikeja", "id": "1"},
                        {"country": "nigeria", "state": "lagos", "city": "victoria island", "id": "2"}],
        "record_ids": ["1", "2"]
    }
    """
    if not isinstance(data, list):
        raise ValueError(
            "Invalid data format. Data argument must be a list of dictionaries"
        )
    if not callable(hash_function):
        raise ValueError(
            "Invalid hash function. Hash function argument must be a callable"
        )
    if not isinstance(table_id, str):
        raise ValueError("Invalid table ID. Table ID argument must be a string")

    try:
        client = bigquery.Client()
        record_list = []
        record_ids = []

        for record in data:
            record = {
                key: value.lower() if isinstance(value, str) else value
                for key, value in record.items()
            }
            hash_key = hash_function(record)["hash_key"]
            record["id"] = hash_key
            record_list.append(record)
            record_ids.append(hash_key)

        table_ref = client.get_table(table_id)

        existing_data = (
            client.list_rows(
                table_ref,
            )
            .to_dataframe()
            .to_dict(orient="records")
        )
        existing_ids = []
        for data in existing_data:
            if data["id"] in record_ids:
                existing_ids.append(data["id"])

        return {
            "status": "success",
            "message": "Existing data queried successfully",
            "body": {
                "existing_data": existing_data,
                "existing_ids": existing_ids,
                "record_list": record_list,
                "record_ids": record_ids,
            },
        }

    except Exception as e:
        return {
            "status": "error",
            "message": "An error occurred while querying existing data",
            "error": str(e),
        }


def create_table(
    dataset_id: str, table_name: str, schema: List[SchemaField]
) -> Dict[str, Any]:
    """
    Function to create a BigQuery table

    Args:
    dataset_id (str): The ID of the dataset
    table_name (str): The name of the table
    schema (List[SchemaField]): A list of schema fields

    Returns:
    Dict[str, Any]: A dictionary containing the status of the operation

    Example:
    >>> dataset_id = 'your_project.your_dataset'
    >>> table_name = 'location_dim'
    >>> schema = [
    ...     SchemaField("id", "STRING", mode="REQUIRED", max_length=64),
    ...     SchemaField("country", "STRING", mode="REQUIRED"),
    ...     SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ... ]
    >>> result = create_table(dataset_id, table_name, schema)
    >>> print(result)
    {
        "status": "success",
        "message": "Table location_dim created successfully in dataset your_project.your_dataset",
        "table_id": "your_project.your_dataset.location_dim"
    }
    """

    try:
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_name)
        get_table = client.get_table(table_ref)

        if get_table:
            logger.info(
                {
                    "status": "success",
                    "message": f"Table {table_name} already exists in dataset {dataset_id}",
                    "table_id": f"{get_table}",
                }
            )
            return {
                "status": "success",
                "message": f"Table {table_name} already exists in dataset {dataset_id}",
                "table_id": f"{get_table}",
            }

    except NotFound:
        table = Table(table_ref, schema=schema)
        table = client.create_table(table)

        if table:
            logger.info(
                {
                    "status": "success",
                    "message": f"Table {table_name} created successfully in dataset {dataset_id}",
                    "table_id": f"{table}",
                }
            )
            return {
                "status": "success",
                "message": f"Table {table_name} created successfully in dataset {dataset_id}",
                "table_id": f"{table}",
            }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Table {table_name} failed to create in dataset {dataset_id}",
            "error": str(e),
        }


def insert_new_records(table_id: str, new_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Function to insert new records into a BigQuery table

    Args:
    table_id (str): The ID of the table
    new_record (Dict[str, Any]): A dictionary containing the new records to be inserted into the table

    Returns:
    Dict[str, Any]: A dictionary containing the status of the operation

    Example:
    >>> table_id = 'your-project.your_dataset.location_dim'
    >>> new_record = {'country': 'Nigeria', 'state': 'Lagos', 'city': 'Ikeja'}
    ...
    >>> result = insert_new_records(dataset_id, table_id, new_record)
    >>> print(result)
    {
        "status": "success",
        "message": "New records inserted successfully",
        "record": new_record
    }
    """
    try:
        client = bigquery.Client()

        tmp_file_name = "/tmp/new_records.json"
        with open(tmp_file_name, "w") as file:
            json.dump(new_record, file)
            file.write("\n")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=False,
        )

        with open(tmp_file_name, "rb") as file:
            job = client.load_table_from_file(file, table_id, job_config=job_config)

        job_result = job.result()

        if job_result:
            os.remove(tmp_file_name)

            logger.info(
                {
                    "status": "success",
                    "message": "New record is inserted successfully",
                    "record": new_record,
                }
            )
            return {
                "status": "success",
                "message": "New record is inserted successfully",
                "record": new_record,
            }

        else:
            logger.info(
                {
                    "status": "error",
                    "message": "Insert records Job wasn't executed successfully",
                    "job_result": job_result,
                }
            )
            return {
                "status": "error",
                "message": "Insert records Job wasn't executed successfully",
                "job_result": job_result,
            }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "An error occurred while inserting new records",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "An error occurred while inserting new records",
            "error": str(e),
        }


def update_fact_record(
    new_record: Dict[str, Any], hash_function: callable
) -> Dict[str, Any]:
    """
    Function to update the fact record for the corresponding dimension record and also the current weather data

    Args:
    new_record (Dict[str, Any]): A dictionary containing the new records to be inserted
    hash_function (callable): A function to generate hash keys for the records

    Returns:
    Dict[str, Any]: A dictionary containing the status of the operation

    Example:
    >>> new_record = {'country': 'Nigeria', 'state': 'Lagos', 'city': 'Ikeja'}
    ...
    >>> result = update_fact_record(new_record, gen_hash_key_location_dim)
    >>> print(result)
    {
        "status": "success",
        "message": "Current weather records and corresponding dimension records updated successfully",
        "record": record_to_update,
    }
    """

    if not callable(hash_function):
        raise ValueError(
            "Invalid hash function. Hash function argument must be a callable"
        )
    if not isinstance(new_record, dict):
        raise ValueError("Invalid data format. Data argument must be a dictionary")

    try:
        record_to_update = {
            "id": hash_function()["hash_key"],
            "location_id": new_record["id"],
            "temperature": new_record["temp"],
            "feels_like": new_record["feels_like"],
            "pressure": new_record["pressure"],
            "humidity": new_record["humidity"],
            "dew_point": new_record["dew_point"],
            "ultraviolet_index": new_record["ultraviolet_index"],
            "clouds": new_record["clouds"],
            "visibility": new_record["visibility"],
            "wind_speed": new_record["wind_speed"],
            "wind_deg": new_record["wind_deg"],
            "sunrise": new_record["sunrise"],
            "sunset": new_record["sunset"],
            "date": new_record["date"].isoformat(),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        print("record_to_update", record_to_update)

        logger.info(
            {
                "status": "success",
                "message": "Current weather records and corresponding dimension records updated successfully",
                "record": record_to_update,
            }
        )
        return {
            "status": "success",
            "message": "Current weather records and corresponding dimension records updated successfully",
            "record": record_to_update,
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "An error occurred while updating corresponding dimension record to the fact record",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "An error occurred while updating corresponding dimension record to the fact record",
            "error": str(e),
        }


def update_existing_fact_record(
    existing_fact_records: List[Dict[str, Any]],
    new_record: Dict[str, Any],
    record_to_update_mapper: Dict[str, Any],
):
    """
    Function to update existing records in a table

    Args:
    existing_fact_records (List[Dict[str, Any]]): A list of dictionaries containing the existing records in the fact table
    new_record (Dict[str, Any]): A dictionary containing the new records to be updated in the table
    record_to_update_mapper (Dict[str, Any]): A dictionary containing the record to update to the fact record

    Returns:
    Dict[str, Any]: A dictionary containing the status of the operation

    Example:
    >>> dim_table_id = 'your-project.your_dataset.location_dim'
    >>> new_record = {'country': 'Nigeria', 'state': 'Lagos', 'city': 'Ikeja'}
    ...
    >>> result = update_dim_record(dim_table_id, new_record, 'id', 'id', record_to_update_mapper)
    >>> print(result)
    {
        "status": "success",
        "message": "New records updated successfully to the dimension table",
        "record": new_record
    }
    """
    if not isinstance(new_record, dict):
        raise ValueError("Invalid data format. Data argument must be a dictionary")
        raise ValueError("Invalid column name. Column name argument must be a string")
    if not isinstance(record_to_update_mapper, dict):
        raise ValueError("Invalid data format. Data argument must be a dictionary")
    if not isinstance(existing_fact_records, List):
        raise ValueError(
            "Invalid data format. Existing dimension records must be a list"
        )

    try:
        existing_fact_record = existing_fact_records[0]
        fact_record_to_update = existing_fact_record

        for key, value in record_to_update_mapper.items():
            fact_record_to_update[key] = new_record[value]

        fact_record_to_update["updated_at"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        logger.info(
            {
                "status": "success",
                "message": "Fact records are updated successfully with the corresponding dimension record",
                "record": fact_record_to_update,
            }
        )
        return {
            "status": "success",
            "message": "Fact records are updated successfully with the corresponding dimension record",
            "record": fact_record_to_update,
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": "An error occurred while updating records in the fact record",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": "An error occurred while updating records in the fact record",
            "error": str(e),
        }


def retrieve_country_code(country: str) -> Dict[str, str]:
    """
    Function to retrieve the country code from the restcountries API

    Args:
    country (str): Name of the country

    Returns:
    Dict[str, str]: A dictionary containing the status of the operation,
                    a message and the country code
    Examples:
    >>> retrieve_country_code("Nigeria")

    {"status": "success",
    "message": "Country code for Nigeria is NG",
    "country_codes": "NG"}

    """
    try:
        url = f"https://restcountries.com/v3.1/name/{country}"
        response = requests.get(url)
        data = response.json()[0]
        country_code = data["cca2"]

        logger.info(
            {
                "status": "success",
                "message": f"Country code for {country} is {country_code}",
                "country_codes": country_code,
            }
        )
        return {
            "status": "success",
            "message": f"Country code for {country} is {country_code}",
            "country_codes": country_code,
        }

    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "message": f"Unable to get country code for {country} from the API",
                "error": str(e),
            }
        )
        return {
            "status": "error",
            "message": f"Unable to get country code for {country} from the API",
            "error": str(e),
        }


def validate_city(
    country_codes: List[str], cities: List[str], country_city_api_key: str
) -> Dict[str, Union[str, Dict[str, str]]]:
    """
    Function to validate the city name

    Args:
    country_code (str): The country code
    city (str): The city name

    Returns:
    A dictionary containing the status of the operation,
    a message and the city name

    """
    if not isinstance(country_codes, list):
        raise ValueError("Invalid country codes. Country codes argument must be a list")
    if not isinstance(cities, list):
        raise ValueError("Invalid cities. Cities argument must be a list")
    try:
        headers = {"X-CSCAPI-KEY": country_city_api_key}

        validated_country_cities_dict = {}
        for country_code in country_codes:
            url = f"https://api.countrystatecity.in/v1/countries/{country_code}/cities"

            response = requests.request("GET", url, headers=headers)
            data = response.json()

            if data:
                country_cities = []

                for city in data:
                    country_cities.append(city["name"].lower())

                valid_cities_list = []

                for city in cities:
                    if city in country_cities:
                        valid_cities_list.append(city)
                        validated_country_cities_dict[country_code] = valid_cities_list

            else:
                error_logger.error(
                    {
                        "status": "error",
                        "message": f"Unable to retrieve cities for the country code: {country_code}",
                        "error": f"Error occurred while retrieving cities for the country code: {country_code} from the API",
                    }
                )
                return {
                    "status": "error",
                    "message": f"Unable to validate the city name for {country_code}",
                    "error": f"City name not found in the country code: {country_code}",
                }

        return {
            "status": "success",
            "message": "City name validated successfully",
            "validated_country_cities": validated_country_cities_dict,
        }

    except Exception as e:
        return {
            "status": "error",
            "message": "An error occurred while validating the city name",
            "error": str(e),
        }
