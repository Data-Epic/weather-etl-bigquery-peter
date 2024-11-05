import datetime
from unittest.mock import MagicMock, patch

import pytest
import unittest
import os
from google.api_core.exceptions import NotFound, Conflict
from requests.exceptions import RequestException
from google.cloud.bigquery import SchemaField
from typing import Dict, List, Any

from helpers.utils import (
    gen_hash_key_datedim,
    gen_hash_key_location_dim,
    gen_hash_key_weather_type_dim,
    gen_hash_key_weatherfact,
    query_bigquery_existing_data,
    update_fact_record,
    update_existing_fact_record,
    retrieve_country_code,
    create_dataset,
    create_table,
    insert_new_records,
    validate_city,
)


@pytest.fixture
def mock_bigquery_client():
    with patch("google.cloud.bigquery.Client") as mock_client:
        yield mock_client


@pytest.fixture
def sample_schema() -> List[SchemaField]:
    """
    Sample schema for testing

    Returns:
        List[SchemaField]: List of SchemaField objects
    """
    return [
        SchemaField("city", "STRING", mode="REQUIRED", max_length=50),
        SchemaField("country", "STRING", mode="REQUIRED", max_length=50),
        SchemaField("state", "STRING", mode="REQUIRED", max_length=50),
        SchemaField("latitude", "FLOAT", mode="REQUIRED"),
        SchemaField("longitude", "FLOAT", mode="REQUIRED"),
        SchemaField("timezone", "STRING", mode="REQUIRED", max_length=50),
        SchemaField("timezone_offset", "INTEGER", mode="REQUIRED"),
        SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]


@pytest.fixture
def sample_data() -> List[Dict[str, Any]]:
    """
    Sample dictionary data for testing

    Returns:
        List[Dict[str, Any]]: List of dictionaries
    """

    return [
        {
            "city": "abuja",
            "country": "ng",
            "state": "lagos state",
            "latitude": 6.46,
            "longitude": 3.39,
            "timezone": "Africa/Lagos",
            "timezone_offset": 3600,
            "date_time": datetime.datetime(2024, 10, 18, 20, 53, 57),
            "date": datetime.date(2024, 10, 18),
            "year": 2024,
            "month": 10,
            "day": 18,
            "day_of_week": "Friday",
            "sunrise": 1729229546,
            "sunset": 1729272617,
            "temp": 299.29,
            "feels_like": 299.29,
            "pressure": 1013,
            "humidity": 89,
            "dew_point": 297.33,
            "ultraviolet_index": 0,
            "clouds": 20,
            "visibility": 10000,
            "wind_speed": 1.54,
            "wind_deg": 190,
            "weather": "Thunderstorm",
            "description": "thunderstorm",
        },
        {
            "city": "ibadan",
            "country": "ng",
            "state": "oyo state",
            "latitude": 7.38,
            "longitude": 3.9,
            "timezone": "Africa/Lagos",
            "timezone_offset": 3600,
            "date_time": datetime.datetime(2024, 10, 18, 20, 53, 58),
            "date": datetime.date(2024, 10, 18),
            "year": 2024,
            "month": 10,
            "day": 18,
            "day_of_week": "Friday",
            "sunrise": 1729229462,
            "sunset": 1729272457,
            "temp": 296.66,
            "feels_like": 297.52,
            "pressure": 1013,
            "humidity": 94,
            "dew_point": 295.64,
            "ultraviolet_index": 0,
            "clouds": 63,
            "visibility": 10000,
            "wind_speed": 0.98,
            "wind_deg": 175,
            "weather": "Clouds",
            "description": "broken clouds",
        },
        {
            "city": "kano",
            "country": "ng",
            "state": "kano state",
            "latitude": 11.99,
            "longitude": 8.53,
            "timezone": "Africa/Lagos",
            "timezone_offset": 3600,
            "date_time": datetime.datetime(2024, 10, 18, 20, 53, 58),
            "date": datetime.date(2024, 10, 18),
            "year": 2024,
            "month": 10,
            "day": 18,
            "day_of_week": "Friday",
            "sunrise": 1729228544,
            "sunset": 1729271153,
            "temp": 302.05,
            "feels_like": 303.72,
            "pressure": 1011,
            "humidity": 58,
            "dew_point": 292.96,
            "ultraviolet_index": 0,
            "clouds": 0,
            "visibility": 10000,
            "wind_speed": 0,
            "wind_deg": 0,
            "weather": "Clear",
            "description": "clear sky",
        },
    ]


@pytest.fixture
def mock_weather_record() -> Dict[str, Any]:
    """
    Sample weather record for testing

    Returns:
        Dict[str, Any]: Dictionary containing weather record
    """
    return {
        "id": "location1",
        "temp": 25.5,
        "feels_like": 26.0,
        "pressure": 1013,
        "humidity": 60,
        "dew_point": 15.5,
        "ultraviolet_index": 7.2,
        "clouds": 20,
        "visibility": 10000,
        "date": datetime.datetime.strptime("2023-05-22", "%Y-%m-%d"),
        "wind_speed": 5.5,
        "wind_deg": 180,
        "sunrise": 1621234567,
        "sunset": 1621191234,
    }


@pytest.fixture
def mock_hash_function() -> Dict[str, str]:
    """
    Mock hash function for testing

    Returns:
        Dict[str, str]: Dictionary containing the hash key
    """

    def _hash_function() -> Dict[str, str]:
        return {"hash_key": "hash1"}

    return _hash_function


@pytest.fixture
def sample_bq_df():
    return [
        {
            "id": "hash1",
            "location_id": "location1",
            "date_id": "date1",
            "weather_type_id": "weather_type1",
            "temperature": 25.5,
            "feels_like": 26.0,
            "pressure": 1013,
            "humidity": 60,
            "dew_point": 15.5,
            "ultraviolet_index": 7.2,
            "clouds": 20,
            "visibility": 10000,
            "date": datetime.strptime("2023-05-22", "%Y-%m-%d"),
            "wind_speed": 5.5,
            "wind_deg": 180,
            "sunrise": 1621234567,
            "sunset": 1621191234,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        {
            "id": "hash2",
            "location_id": "location2",
            "date_id": "date2",
            "weather_type_id": "weather_type2",
            "temperature": 26.5,
            "feels_like": 27.0,
            "pressure": 1014,
            "humidity": 61,
            "dew_point": 16.5,
            "ultraviolet_index": 8.2,
            "clouds": 21,
            "visibility": 10001,
            "date": datetime.strptime("2023-05-23", "%Y-%m-%d"),
            "wind_speed": 6.5,
            "wind_deg": 190,
            "sunrise": 1621234568,
            "sunset": 1621191235,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    ]


def test_gen_hash_key_weatherfact() -> None:
    """
    Tests the gen_hash_key_weatherfact function for a successful case

    Returns:
        None
    """
    result = gen_hash_key_weatherfact()
    assert result["status"] == "success"
    assert result["message"] == "Hash key generated successfully"
    assert isinstance(result["hash_key"], str)
    assert len(result["hash_key"]) == 64


def test_gen_hash_key_location_dim_success() -> None:
    """
    Tests the gen_hash_key_location_dim function for a successful case

    Returns:
        None
    """
    data = {
        "country": "USA",
        "state": "California",
        "city": "Los Angeles",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "timezone": "America/Los_Angeles",
        "timezone_offset": -25200,
    }

    result = gen_hash_key_location_dim(data)
    assert result["status"] == "success"
    assert result["message"] == "Hash key generated successfully"
    assert isinstance(result["hash_key"], str)
    assert len(result["hash_key"]) == 64


def test_gen_hash_key_location_dim_case_insensitive() -> None:
    """
    Tests the gen_hash_key_location_dim function for case-insensitive input

    Returns:
        None
    """
    data1 = {
        "country": "USA",
        "state": "California",
        "city": "Los Angeles",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "timezone": "America/Los_Angeles",
        "timezone_offset": -25200,
    }

    data2 = {
        "country": "USA",
        "state": "CALIFORNIA",
        "city": "Los AngEles",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "timezone": "America/Los_Angeles",
        "timezone_offset": -25200,
    }

    result1 = gen_hash_key_location_dim(data1)
    result2 = gen_hash_key_location_dim(data2)
    assert result1["hash_key"] == result2["hash_key"]


def test_gen_hash_key_location_dim_invalid_input() -> None:
    """
    Tests the gen_hash_key_location_dim function with invalid input type

    Returns:
        None
    """

    result = gen_hash_key_location_dim("invalid")
    assert result["status"] == "error"
    assert (
        result["message"]
        == "Invalid data format. Data argument of the location data must be a dictionary"
    )


def test_gen_hash_key_location_dim_missing_key() -> None:
    """
    Tests the gen_hash_key_location_dim function with missing keys

    Returns:
        None
    """
    data = {"country": "USA", "state": "California", "latitude": 34.0522}
    result = gen_hash_key_location_dim(data)
    assert result["status"] == "error"
    assert result["message"] == "Unable to generate hash key for location data"
    assert result["error"] == "'longitude'"


def test_gen_hash_key_datedim_success() -> None:
    """
    Tests the gen_hash_key_datedim function for a successful case

    Returns:
        None
    """
    data = {"date": "2023-05-22"}
    result = gen_hash_key_datedim(data)
    assert result["status"] == "success"
    assert result["message"] == "Hash key generated successfully"
    assert isinstance(result["hash_key"], str)
    assert len(result["hash_key"]) == 64


def test_gen_hash_key_datedim_invalid_input() -> None:
    """
    Tests the gen_hash_key_datedim function with an invalid input type

    Returns:
        None
    """
    result = gen_hash_key_datedim("invalid")
    assert result["status"] == "error"
    assert (
        result["message"]
        == "Invalid data format. Data argument for date must be a dictionary"
    )


def test_gen_hash_key_datedim_missing_key() -> None:
    """
    Tests the gen_hash_key_datedim function with missing keys

    Returns:
        None
    """
    data = {"wrong_key": "2023-05-22"}
    result = gen_hash_key_datedim(data)
    assert result["status"] == "error"
    assert result["message"] == "Unable to generate hash key for date data"
    assert result["error"] == "'date'"


def test_gen_hash_key_weather_type_dim_success() -> None:
    """
    Tests the gen_hash_key_weather_type_dim function for a successful case

    Returns:
        None
    """
    data = {"weather": "Sunny"}
    result = gen_hash_key_weather_type_dim(data)
    assert result["status"] == "success"
    assert result["message"] == "Hash key generated successfully"
    assert isinstance(result["hash_key"], str)
    assert len(result["hash_key"]) == 64


def test_gen_hash_key_weather_type_dim_case_insensitive() -> None:
    """
    Tests the gen_hash_key_weather_type_dim function for case-insensitive input

    Returns:
        None
    """
    data1 = {"weather": "Sunny"}
    data2 = {"weather": "SUNNY"}
    result1 = gen_hash_key_weather_type_dim(data1)
    result2 = gen_hash_key_weather_type_dim(data2)
    assert result1["hash_key"] == result2["hash_key"]


def test_gen_hash_key_weather_type_dim_invalid_input() -> None:
    """
    Tests the gen_hash_key_weather_type_dim function with invalid input

    Returns:
        None
    """
    result = gen_hash_key_weather_type_dim("invalid")
    assert result["status"] == "error"
    assert (
        result["message"]
        == "Invalid data format. Data argument for weather must be a dictionary"
    )


def test_gen_hash_key_weather_type_dim_missing_key() -> None:
    """
    Tests the gen_hash_key_weather_type_dim function with missing keys

    Returns:
        None
    """
    data = {"wrong_key": "Sunny"}
    result = gen_hash_key_weather_type_dim(data)
    assert result["status"] == "error"
    assert result["message"] == "Unable to generate hash key for weather data"
    assert result["error"] == "'weather'"


def test_hash_consistency() -> None:
    """
    Tests the consistency of the hash key generation function

    Returns:
        None
    """
    data = {
        "country": "USA",
        "state": "California",
        "city": "Los Angeles",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "timezone": "America/Los_Angeles",
        "timezone_offset": -25200,
    }
    result1 = gen_hash_key_location_dim(data)
    result2 = gen_hash_key_location_dim(data)
    assert result1["hash_key"] == result2["hash_key"]


def test_hash_uniqueness() -> None:
    """
    Tests the uniqueness of the hash key generation function

    Returns:
        None
    """
    data1 = {
        "country": "USA",
        "state": "California",
        "city": "Los Angeles",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "timezone": "America/Los_Angeles",
        "timezone_offset": -25200,
    }
    data2 = {
        "country": "USA",
        "state": "California",
        "city": "San Francisco",
        "latitude": 37.7749,
        "longitude": -122.4194,
        "timezone": "America/Los_Angeles",
        "timezone_offset": -25200,
    }
    result1 = gen_hash_key_location_dim(data1)
    result2 = gen_hash_key_location_dim(data2)
    assert result1["hash_key"] != result2["hash_key"]


def test_hash_key_length_and_type() -> None:
    """
    Tests the location dimension hash key generation function for the correct length and type

    Returns:
        None
    """
    data1 = {
        "country": "USA",
        "state": "California",
        "city": "Los Angeles",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "timezone": "America/Los_Angeles",
        "timezone_offset": -25200,
    }
    data2 = {
        "country": "USA",
        "state": "California",
        "city": "San Francisco",
        "latitude": 37.7749,
        "longitude": -122.4194,
        "timezone": "America/Los_Angeles",
        "timezone_offset": -25200,
    }

    result1 = gen_hash_key_location_dim(data1)
    result2 = gen_hash_key_location_dim(data2)
    assert result1["status"] == "success"
    assert result2["status"] == "success"
    assert len(result1["hash_key"]) == 64
    assert len(result2["hash_key"]) == 64
    assert isinstance(result1["hash_key"], str)
    assert isinstance(result2["hash_key"], str)


def test_create_dataset_success(mock_bigquery_client) -> None:
    """
    Tests the create_dataset function for a successful case

    Args:
        mock_bigquery_client (MagicMock): Pytest mocker object

    Returns:
        None
    """
    project_id = "test_project"
    dataset_id = "test_dataset"
    mock_dataset = MagicMock()
    mock_bigquery_client.return_value.dataset.return_value = mock_dataset
    mock_bigquery_client.return_value.create_dataset.return_value = mock_dataset
    mock_bigquery_client.return_value.get_dataset.return_value = (
        f"{project_id}.{dataset_id}"
    )

    result = create_dataset(project_id, dataset_id)
    assert result["status"] == "success"
    assert result["message"] == "Dataset created successfully"
    assert result["dataset_id"] == f"{project_id}.{dataset_id}"
    mock_bigquery_client.return_value.create_dataset.assert_called_once()


def test_create_dataset_already_exists(mock_bigquery_client) -> None:
    """
    Tests the create_dataset function when the dataset already exists

    Args:
        mock_bigquery_client (MagicMock): Pytest mocker object

    Returns:
        None
    """

    project_id = "test_project"
    dataset_id = "test_dataset"

    mock_bigquery_client.return_value.create_dataset.side_effect = Conflict(
        "Dataset already exists"
    )

    result = create_dataset(project_id, dataset_id)

    assert result["status"] == "success"
    assert result["message"] == "Dataset already exists"
    assert result["dataset_id"] == dataset_id


def test_create_dataset_error(mock_bigquery_client) -> None:
    """
    Tests the create_dataset function when an error occurs

    Args:
        mock_bigquery_client (MagicMock): Pytest mocker object for Bigquery client

    Returns:
        None
    """

    project_id = "test_project"
    dataset_id = "test_dataset"

    mock_bigquery_client.return_value.create_dataset.side_effect = Exception(
        "Bigquery Error"
    )

    result = create_dataset(project_id, dataset_id)

    assert result["status"] == "error"
    assert result["message"] == "Unable to create dataset"
    assert "Bigquery Error" in result["error"]


def test_query_bigquery_existing_data_success(
    mock_bigquery_client: MagicMock, sample_data: List[Dict[str, Any]]
) -> None:
    """
    Tests the query_bigquery_existing_data function for a successful case

    Args:
        mock_bigquery_client (MagicMock): Pytest mocker object for Bigquery client
        sample_data (List[Dict[str, Any]]): Sample data

    Returns:
        None
    """
    table_id = "test_dataset.test_table"
    mock_dict = [
        {
            "id": "hash1",
            "city": "abuja",
            "country": "ng",
            "state": "lagos state",
            "latitude": 6.46,
            "longitude": 3.39,
            "timezone": "Africa/Lagos",
            "timezone_offset": 3600,
            "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        {
            "id": "hash2",
            "city": "ibadan",
            "country": "ng",
            "state": "oyo state",
            "latitude": 7.38,
            "longitude": 3.9,
            "timezone": "Africa/Lagos",
            "timezone_offset": 3600,
            "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
        {
            "id": "hash3",
            "city": "kano",
            "country": "ng",
            "state": "kano state",
            "latitude": 11.99,
            "longitude": 8.53,
            "timezone": "Africa/Lagos",
            "timezone_offset": 3600,
            "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    ]

    mock_bigquery_client.return_value.list_rows.return_value.to_dataframe.return_value.to_dict.return_value = mock_dict

    def mock_hash_function(record) -> Dict[str, str]:
        """
        Mock hash function for the query_bigquery_existing_data function

        Args:
            record (Dict[str, Any]): Record to be hashed

        Returns:
            Dict[str, str]: Dictionary containing the hash key
        """
        hash_dict = {}

        if record["city"] == "abuja":
            hash_dict["hash_key"] = "hash1"
        elif record["city"] == "ibadan":
            hash_dict["hash_key"] = "hash2"
        else:
            hash_dict["hash_key"] = "hash3"

        return hash_dict

    result = query_bigquery_existing_data(table_id, sample_data, mock_hash_function)

    assert result["status"] == "success"
    assert len(result["body"]["existing_data"]) == 3
    assert len(result["body"]["existing_ids"]) == 3
    assert len(result["body"]["record_list"]) == 3
    assert len(result["body"]["record_ids"]) == 3
    assert result["body"]["existing_ids"] == ["hash1", "hash2", "hash3"]


def test_bigquery_existing_data_invalid_data() -> None:
    """
    Tests the query_bigquery_existing_data function with an invalid data as input

    Returns:
        None
    """
    table_id = "test_dataset.test_table"
    sample_data = "invalid"

    def mock_hash_function(record) -> Dict[str, str]:
        """
        Mock hash function for the query_bigquery_existing_data function

        Args:
            record (Dict[str, Any]): Record to be hashed

        Returns:
            Dict[str, str]: Dictionary containing the hash key
        """
        return {"hash_key": "hash1"}

    with pytest.raises(
        ValueError,
        match="Invalid data format. Data argument must be a list of dictionaries",
    ):
        query_bigquery_existing_data(table_id, sample_data, mock_hash_function)


def test_create_table_success(
    mock_bigquery_client: MagicMock,
    sample_schema: List[SchemaField],
) -> None:
    """
    Tests the create_table function for a successful case

    Args:
        mock_bigquery_client (MagicMock): Pytest mocker object for Bigquery client
        sample_data (List[Dict[str, Any]]): Sample data

    Returns:
        None
    """
    dataset_id = "test_dataset"
    table_name = "test_table"

    mock_table = MagicMock()
    mock_bigquery_client.return_value.dataset.return_value.table.return_value = (
        mock_table
    )
    mock_bigquery_client.return_value.get_table.side_effect = NotFound(
        "Table not found"
    )
    mock_bigquery_client.return_value.create_table.return_value = (
        f"{dataset_id}.{table_name}"
    )

    result = create_table(dataset_id, table_name, sample_schema)

    assert result["status"] == "success"
    assert (
        result["message"]
        == f"Table {table_name} created successfully in dataset {dataset_id}"
    )
    assert result["table_id"] == f"{dataset_id}.{table_name}"

    mock_bigquery_client.return_value.create_table.assert_called_once()


def test_create_table_already_exists(
    mock_bigquery_client: MagicMock,
    sample_schema: List[SchemaField],
) -> None:
    """
    Tests the create_table function when the table already exists

    Args:
        mock_bigquery_client (MagicMock): Pytest mocker object for Bigquery client
        sample_data (List[Dict[str, Any]]): Sample data

    Returns:
        None
    """
    dataset_id = "test_dataset"
    table_name = "test_table"

    mock_table = MagicMock()
    mock_bigquery_client.return_value.dataset.return_value.table.return_value = (
        mock_table
    )
    mock_bigquery_client.return_value.get_table.return_value = (
        f"{dataset_id}.{table_name}"
    )

    result = create_table(dataset_id, table_name, sample_schema)

    assert result["status"] == "success"
    assert (
        result["message"]
        == f"Table {table_name} already exists in dataset {dataset_id}"
    )
    assert result["table_id"] == f"{dataset_id}.{table_name}"


def test_create_table_error(
    mock_bigquery_client: MagicMock,
    sample_schema: List[SchemaField],
) -> None:
    """
    Tests the create_table function when an error occurs

    Args:
        mock_bigquery_client (MagicMock): Pytest mocker object for Bigquery client
        sample_data (List[Dict[str, Any]]): Sample data

    Returns:
        None
    """
    dataset_id = "test_dataset"
    table_name = "test_table"

    mock_table = MagicMock()
    mock_bigquery_client.return_value.dataset.return_value.table.return_value = (
        mock_table
    )
    mock_bigquery_client.return_value.get_table.side_effect = Exception(
        "Error occured while creating table"
    )

    result = create_table(dataset_id, table_name, sample_schema)

    assert result["status"] == "error"
    assert (
        result["message"]
        == f"Table {table_name} failed to create in dataset {dataset_id}"
    )
    assert "Error occured while creating table" in result["error"]


def test_insert_new_records_success(mock_bigquery_client: MagicMock) -> None:
    """
    Tests the insert_data_to_fact_table function for a successful case

    Args:
        mock_bigquery_client (MagicMock): Pytest mocker object for Bigquery client

    Returns:
        None
    """

    table_id = "test_dataset.test_table"
    new_record = {
        "country": "USA",
        "state": "California",
        "city": "Los Angeles",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "timezone": "America/Los_Angeles",
        "timezone_offset": -25200,
        "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    mock_job = MagicMock()
    mock_job.result.return_value = True
    mock_bigquery_client.return_value.load_table_from_file.return_value = mock_job

    result = insert_new_records(table_id, new_record)

    assert result["status"] == "success"
    assert result["message"] == "New record is inserted successfully"
    assert result["record"] == new_record
    assert not os.path.exists("/tmp/new_records.json")


def test_insert_new_records_error(mock_bigquery_client: MagicMock) -> None:
    """
    Tests the insert_new_records function when an error occurs

    Args:
        mock_bigquery_client (MagicMock): Pytest mocker object

    Returns:
        None
    """

    table_id = "test_dataset.test_table"
    new_record = {
        "country": "USA",
        "state": "California",
        "city": "Los Angeles",
        "latitude": 34.0522,
        "longitude": -118.2437,
        "timezone": "America/Los_Angeles",
        "timezone_offset": -25200,
        "created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    mock_bigquery_client.return_value.load_table_from_file.side_effect = Exception(
        "Unable to insert new record"
    )

    result = insert_new_records(table_id, new_record)

    assert result["status"] == "error"
    assert "An error occurred" in result["message"]


class TestUpdateFactRecord(unittest.TestCase):
    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        self.valid_new_record = {
            "id": "loc123",
            "temp": 25.5,
            "feels_like": 26.0,
            "pressure": 1013,
            "humidity": 65,
            "dew_point": 18.5,
            "ultraviolet_index": 5,
            "clouds": 40,
            "visibility": 10000,
            "wind_speed": 3.5,
            "wind_deg": 180,
            "sunrise": "06:00:00",
            "sunset": "18:00:00",
            "date": datetime.datetime.now(),
        }

        self.mock_hash_function = MagicMock(return_value={"hash_key": "hash123"})

    def test_update_fact_record_success(self) -> None:
        """
        Test successful fact record update with valid inputs.

        Returns:
            None

        """
        result = update_fact_record(self.valid_new_record, self.mock_hash_function)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["record"]["id"], "hash123")
        self.assertEqual(result["record"]["location_id"], "loc123")
        self.assertEqual(result["record"]["temperature"], 25.5)

    def test_update_fact_record_invalid_hash_function(self):
        """
        Test with invalid hash function (non-callable).

        Returns:
            None

        """
        with self.assertRaises(ValueError) as context:
            update_fact_record(self.valid_new_record, "not_a_function")

        self.assertIn("Invalid hash function", str(context.exception))

    def test_update_fact_record_invalid_record_type(self):
        """
        Test with invalid record type (non-dictionary).

        Returns:
            None
        """

        with self.assertRaises(ValueError) as context:
            update_fact_record(["invalid_type"], self.mock_hash_function)

        self.assertIn("Invalid data format", str(context.exception))

    def test_update_fact_record_missing_required_fields(self):
        """
        Test with missing required fields in the record.

        Returns:
            None
        """
        invalid_record = {"id": "loc123"}

        result = update_fact_record(invalid_record, self.mock_hash_function)
        self.assertEqual(result["status"], "error")
        self.assertIn("error", result)


class TestUpdateExistingFactRecord(unittest.TestCase):
    """
    Test for the update_existing_fact_record function in utils.py
    It tests the function with valid and invalid inputs.

    """

    def setUp(self):
        self.valid_new_record = {
            "id": "loc123",
            "temp": 25.5,
            "feels_like": 26.0,
            "pressure": 1013,
            "humidity": 65,
            "dew_point": 18.5,
            "ultraviolet_index": 5,
            "clouds": 40,
            "visibility": 10000,
            "wind_speed": 3.5,
            "wind_deg": 180,
            "sunrise": "06:00:00",
            "sunset": "18:00:00",
            "date": datetime.datetime.now(),
        }

        self.existing_fact_records = [
            {
                "id": "fact123",
                "location_id": "loc123",
                "temperature": 24.0,
                "feels_like": 25.0,
                "pressure": 1012,
                "humidity": 60,
                "created_at": "2024-01-01 00:00:00",
                "updated_at": "2024-01-01 00:00:00",
            }
        ]

        self.record_mapper = {
            "temperature": "temp",
            "feels_like": "feels_like",
            "pressure": "pressure",
            "humidity": "humidity",
        }

    def test_update_existing_fact_record_success(self) -> None:
        """Test successful update of existing fact record."""
        result = update_existing_fact_record(
            self.existing_fact_records, self.valid_new_record, self.record_mapper
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["record"]["temperature"], 25.5)
        self.assertEqual(result["record"]["humidity"], 65)

    def test_update_existing_fact_record_invalid_record_type(self) -> None:
        """
        Test with invalid record type for existing records.

        Returns:
            None
        """
        with self.assertRaises(ValueError) as context:
            update_existing_fact_record(
                "invalid_records", self.valid_new_record, self.record_mapper
            )

        self.assertIn("must be a list", str(context.exception))

    def test_update_existing_fact_record_invalid_mapper(self):
        """
        Test with invalid mapper type.

        Returns:
            None

        """
        with self.assertRaises(ValueError) as context:
            update_existing_fact_record(
                self.existing_fact_records, self.valid_new_record, "invalid_mapper"
            )

        self.assertIn("must be a dictionary", str(context.exception))

    def test_update_existing_fact_record_empty_records(self):
        """
        Test with empty existing records list.

        Returns:
            None

        """
        result = update_existing_fact_record(
            [], self.valid_new_record, self.record_mapper
        )

        self.assertEqual(result["status"], "error")

    def test_update_existing_fact_record_invalid_mapping(self):
        """
        Test with invalid field mapping in mapper.

        Returns:
            None

        """
        invalid_mapper = {"temperature": "nonexistent_field"}

        result = update_existing_fact_record(
            self.existing_fact_records, self.valid_new_record, invalid_mapper
        )

        self.assertEqual(result["status"], "error")


def test_retrieve_country_code_success() -> None:
    """
    Tests the retrieve_country_code function for a successful case

    Returns:
        None
    """
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        country_code = "NG"
        mock_response.json.return_value = [{"cca2": country_code}]
        mock_get.return_value = mock_response

        country = "Nigeria"
        result = retrieve_country_code(country)

        assert result["status"] == "success"
        assert result["message"] == f"Country code for {country} is {country_code}"
        assert result["country_codes"] == f"{country_code}"


def test_retrieve_country_code_api_error():
    """
    Tests the retrieve_country_code function when an error occurs

    Returns:
        None
    """
    with patch("requests.get") as mock_get:
        mock_get.side_effect = Exception("API Error")

        country = "Nigeria"
        result = retrieve_country_code(country)

        assert result["status"] == "error"
        assert (
            result["message"]
            == f"Unable to get country code for {country} from the API"
        )
        assert "API Error" in result["error"]


@pytest.fixture
def mock_city_api_response() -> List[Dict[str, Any]]:
    """
    Mock city API response for testing

    Returns:
        List[Dict[str, Any]]: List of dictionaries containing city data
    """

    return [{"name": "Lagos"}, {"name": "Ikeja"}, {"name": "Victoria Island"}]


def test_validate_city_success(mock_city_api_response) -> None:
    """
    Tests the validate_city function for a successful case

    Args:
        mock_city_api_response (List[Dict[str, Any]]): Mocked city API response data

    Returns:
        None
    """
    with patch("requests.request") as mock_request:
        country_code = ["NG"]
        cities = ["lagos", "ikeja"]
        api_key = "test-api-key"

        mock_response = MagicMock()
        mock_response.json.return_value = mock_city_api_response
        mock_request.return_value = mock_response

        result = validate_city(country_code, cities, api_key)

        assert result["status"] == "success"
        assert result["message"] == "City name validated successfully"
        assert result["validated_country_cities"] == {"NG": ["lagos", "ikeja"]}


def test_validate_city_invalid_country_codes() -> None:
    """
    Tests the validate_city function with invalid country codes

    Returns:
        None
    """

    invalid_country_codes = "NG"
    cities = ["lagos"]
    api_key = "test-api-key"

    with pytest.raises(ValueError, match="Invalid country codes"):
        validate_city(invalid_country_codes, cities, api_key)


def test_validate_city_invalid_cities() -> None:
    """
    Tests the validate_city function with invalid cities

    Returns:
        None
    """

    country_codes = ["NG"]
    invalid_cities = "lagos"
    api_key = "test-api-key"

    with pytest.raises(ValueError, match="Invalid cities"):
        validate_city(country_codes, invalid_cities, api_key)


def test_validate_city_api_error() -> None:
    """
    Tests the validate_city function when an API error occurs

    Returns:
        None
    """
    with patch("requests.request") as mock_request:
        country_codes = ["NG"]
        cities = ["lagos"]
        api_key = "test-api-key"

        mock_request.side_effect = RequestException("API error")

        result = validate_city(country_codes, cities, api_key)

        assert result["status"] == "error"
        assert "error occurred" in result["message"]
        assert "error" in result


def test_validate_city_empty_response() -> None:
    """
    Tests the validate_city function when the city API response is empty

    Returns:
        None
    """
    with patch("requests.request") as mock_request:
        country_code = ["XX"]
        cities = ["nonexistent"]
        api_key = "test-api-key"

        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_request.return_value = mock_response

        result = validate_city(country_code, cities, api_key)

        assert result["status"] == "error"
        assert (
            result["message"]
            == f"Unable to validate the city name for {country_code[0]}"
        )
        assert (
            result["error"]
            == f"City name not found in the country code: {country_code[0]}"
        )


def test_validate_city_multiple_countries(mock_city_api_response) -> None:
    """
    Tests the validate_city function for multiple countries

    Args:
        mock_city_api_response (List[Dict[str, Any]]): Mocked city API response data

    Returns:
        None
    """
    with patch("requests.request") as mock_request:
        country_codes = ["NG", "GH"]
        cities = ["lagos", "accra"]
        api_key = "test-api-key"

        mock_response_ng = MagicMock()
        mock_response_ng.json.return_value = mock_city_api_response
        mock_response_gh = MagicMock()
        mock_response_gh.json.return_value = [{"name": "Accra"}, {"name": "Kumasi"}]
        mock_request.side_effect = [mock_response_ng, mock_response_gh]

        result = validate_city(country_codes, cities, api_key)

        assert result["status"] == "success"
        assert "NG" in result["validated_country_cities"]
        assert "GH" in result["validated_country_cities"]
        assert "lagos" in result["validated_country_cities"]["NG"]
        assert "accra" in result["validated_country_cities"]["GH"]
