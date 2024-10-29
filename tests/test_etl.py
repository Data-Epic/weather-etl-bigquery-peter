import datetime
from unittest.mock import MagicMock, patch
from helpers.utils import create_dataset
import pytest
from typing import Dict

from helpers.weather_etl import (
    get_country_code,
    get_weather_fields,
    restructure_geographical_data,
    merge_current_weather_data,
    transform_weather_records,
    load_records_to_location_dim,
)


@pytest.fixture
def sample_weather_data():
    return [
        {
            "id": "location123",
            "city": "New York",
            "country": "US",
            "state": "NY",
            "latitude": 40.7128,
            "longitude": -74.0060,
            "timezone": "America/New_York",
            "timezone_offset": -14400,
            "temp": 15.5,
            "feels_like": 14.8,
            "pressure": 1015,
            "humidity": 76,
            "dew_point": 11.2,
            "ultraviolet_index": 4.5,
            "clouds": 75,
            "visibility": 10000,
            "wind_speed": 3.6,
            "wind_deg": 180,
            "weather": "Clouds",
            "sunrise": 1635766800,
            "sunset": 1635803400,
            "description": "broken clouds",
            "date": datetime.datetime.now().date(),
        }
    ]


project_id = "weather-etl"
dataset_name = "test_dataset"

create_bq_dataset = create_dataset(project_id, dataset_name)
dataset_id = create_bq_dataset["dataset_id"]


class TestGetCountryCode:
    @pytest.fixture
    def mock_retrieve_country_code(self):
        """
        Mock the retrieve_country_code function

        Returns:
            MagicMock: Mocked response
        """
        with patch("helpers.weather_etl.retrieve_country_code") as mock_response:
            mock_response.return_value = {"country_codes": "US"}
            yield mock_response

    def test_country_success(self) -> None:
        """
        Test for successful retrieval of country code

        Returns:
            None
        """
        country_name = "United States of America"
        result = get_country_code(country_name)
        country_code = ["US"]
        assert result["status"] == "success"
        assert result["country_codes"] == country_code
        assert (
            result["message"]
            == f"Country code(s) for {country_name} are {country_code}"
        )

    def test_invalid_inputs(self, mock_retrieve_country_code) -> None:
        """
        Test for invalid inputs to the get_country_code function

        Args:
            mock_retrieve_country_code (MagicMock): Mocked retrieve_country_code pytest fixture

        Returns:
            None
        """
        mock_retrieve_country_code.side_effect = Exception(
            "An error occured while retrieving country code"
        )
        invalid_input = "invaid country name"
        result = get_country_code(invalid_input)

        assert result["status"] == "error"
        assert (
            result["message"]
            == "Unable to get country code(s) for invaid country name from the API"
        )
        assert result["error"] == "An error occured while retrieving country code"

    def test_api_error(self, mock_retrieve_country_code) -> None:
        """
        Test for API error in the get_country_code function

        Args:
            mock_retrieve_country_code (MagicMock): Mocked retrieve_country_code pytest fixture

        Returns:
            None
        """
        mock_retrieve_country_code.side_effect = Exception(
            "An error occured while retrieving country code"
        )
        country_name = "south africa"
        result = get_country_code(country_name)
        assert result["status"] == "error"
        assert (
            result["message"]
            == f"Unable to get country code(s) for {country_name} from the API"
        )
        assert result["error"] == "An error occured while retrieving country code"


class TestGetWeatherFields:
    @pytest.fixture
    def mock_requests(self):
        with patch("requests.get") as mock_response:
            yield mock_response

    @pytest.fixture
    def mock_validate_city(self):
        with patch("helpers.weather_etl.validate_city") as mock_response:
            yield mock_response

    def test_successful_retrieval(self, mock_requests, mock_validate_city) -> None:
        """
        Test for successful retrieval of the get_weather_fields function

        Args:
            mock_requests (MagicMock): Mocked requests pytest fixture

        Returns:
            None
        """
        mock_response = MagicMock()
        mock_validate_city.return_value = {
            "status": "success",
            "message": "City name validated successfully",
            "validated_country_cities": {"NG": ["lagos"]},
        }
        mock_response.json.return_value = [
            {"name": "Lagos", "lat": 6.5244, "lon": 3.3792}
        ]

        mock_requests.return_value = mock_response

        result = get_weather_fields(
            country_codes=["NG"],
            cities=["Lagos"],
            fields=["name", "lat", "lon"],
            api_key="test_key",
            country_city_api_key="test_key",
        )

        assert result["status"] == "success"
        assert isinstance(result["weather_fields"], list)
        assert result["weather_fields"][0]["name"] == "Lagos"

    def test_invalid_inputs(self) -> None:
        """
        Test for invalid inputs to the get_weather_fields function

        Args:
            None

        Returns:
            None
        """
        country_codes = 123
        cities = "Lagos"
        result = get_weather_fields(
            country_codes,
            cities,
            fields=None,
            api_key="test_key",
            country_city_api_key="test_key",
        )

        assert result["status"] == "error"
        assert (
            result["message"]
            == "Unable to get weather information for Lagos from the API"
        )
        assert result["error"] == "Invalid fields. Fields argument must be a list"

    def test_api_error(self, mock_requests) -> None:
        """
        Test for API error in the get_weather_fields function

        Args:
            mock_requests (MagicMock): Mocked requests pytest fixture

        Returns:
            None
        """

        mock_requests.side_effect = Exception("API Error")

        cities = ["Lagos", "Abuja"]

        result = get_weather_fields(
            country_codes=["NG"],
            cities=cities,
            fields=["name"],
            api_key="test_key",
            country_city_api_key="test_key",
        )

        assert result["status"] == "error"
        assert (
            result["message"]
            == f"Unable to get weather information for {cities} from the API"
        )


class TestRestructureGeographicalData:
    @pytest.fixture
    def valid_weather_records(self):
        return [
            {
                "name": "lagos",
                "lat": 6.46,
                "lon": 3.39,
                "country": "Nigeria",
                "state": "Lagos",
            }
        ]

    def test_successful_restructuring(self, valid_weather_records) -> None:
        """
        Test for successful restructuring of geographical data

        Args:
            valid_weather_records (List[Dict]): Valid weather records

        Returns:
            None

        """
        result = restructure_geographical_data(valid_weather_records)

        assert result["status"] == "success"
        assert "weather_fields" in result["weather_fields"]
        assert "lon_lat" in result["weather_fields"]

        weather_fields = result["weather_fields"]["weather_fields"][0]

        assert weather_fields["city"] == "lagos"
        assert weather_fields["country"] == "Nigeria"
        assert weather_fields["state"] == "Lagos"

        lon_lat = result["weather_fields"]["lon_lat"][0]
        assert lon_lat == (3.39, 6.46)

    def test_missing_required_keys(self) -> None:
        """
        Test for missing required keys in the input data to restructure_geographical_data

        Args:
            None

        Returns:
            None
        """
        invalid_records = [
            {"name": "Lagos", "lat": 6.46, "country": "Nigeria", "state": "Lagos"}
        ]
        result = restructure_geographical_data(invalid_records)

        assert result["status"] == "error"
        assert "Invalid keys" in result["message"]
        assert result["error"] == "Invalid keys: ['name', 'lat', 'country', 'state']"


class TestMergeCurrentWeatherData:
    @pytest.fixture
    def mock_requests(self):
        with patch("requests.get") as mock:
            yield mock

    @pytest.fixture
    def valid_inputs(self):
        return {
            "lon_lat": [(3.39, 6.46)],
            "excluded_fields": "minutely,hourly",
            "weather_fields": [
                {"city": "Lagos", "country": "Nigeria", "state": "Lagos"}
            ],
            "api_key": "test_key",
        }

    def test_successful_merge(self, mock_requests, valid_inputs) -> None:
        """
        Test for successful merging of current weather data

        Args:
            mock_requests (MagicMock): Mocked requests pytest fixture
            valid_inputs (Dict): Valid inputs to the merge_current_weather_data function

        Returns:
            None
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "lat": 6.46,
            "lon": 3.39,
            "timezone": "Africa/Lagos",
            "current": {"temp": 300.15},
        }
        mock_requests.return_value = mock_response

        result = merge_current_weather_data(**valid_inputs)

        assert result["status"] == "success"
        assert len(result["weather_records"]) == 1
        weather_record = result["weather_records"][0]
        assert weather_record["city"] == "Lagos"
        assert weather_record["current"]["temp"] == 300.15
        assert weather_record == {
            "lat": 6.46,
            "lon": 3.39,
            "timezone": "Africa/Lagos",
            "current": {"temp": 300.15},
            "city": "Lagos",
            "country": "Nigeria",
            "state": "Lagos",
        }

    def test_invalid_inputs(self) -> None:
        """
        Test for invalid inputs to the merge_current_weather_data function

        Returns:
            None
        """

        invalid_input = {
            "lon_lat": "invalid_string",
            "excluded_fields": None,
            "weather_fields": [],
            "api_key": "test_key",
        }
        result = merge_current_weather_data(**invalid_input)

        assert result["status"] == "error"
        assert "Invalid input type" in result["message"]

    def test_empty_api_response(self, mock_requests, valid_inputs) -> None:
        """
        Test for empty API response in the merge_current_weather_data function

        Args:
            mock_requests (MagicMock): Mocked requests pytest fixture
            valid_inputs (Dict): Valid inputs to the merge_current_weather_data function

        Returns:
            None
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_requests.return_value = mock_response

        result = merge_current_weather_data(**valid_inputs)

        assert result["status"] == "success"
        assert list(result["weather_records"][0].keys()) == ["city", "country", "state"]


class TestTransformWeatherRecords:
    @pytest.fixture
    def sample_weather_records(self):
        return [
            {
                "city": "New York",
                "country": "US",
                "state": "NY",
                "lat": 40.7128,
                "lon": -74.0060,
                "timezone": "America/New_York",
                "timezone_offset": -14400,
                "current": {
                    "dt": 1635724800,
                    "sunrise": 1635766800,
                    "sunset": 1635803400,
                    "temp": 15.5,
                    "feels_like": 14.8,
                    "pressure": 1015,
                    "humidity": 76,
                    "dew_point": 11.2,
                    "uvi": 4.5,
                    "clouds": 75,
                    "visibility": 10000,
                    "wind_speed": 3.6,
                    "wind_deg": 180,
                    "weather": [{"main": "Clouds", "description": "broken clouds"}],
                },
            }
        ]

    def test_successful_transformation(self, sample_weather_records) -> None:
        """
        Test for successful transformation of weather records

        Args:
            sample_weather_records (List[Dict]): Sample weather records to transform

        Returns:
            None
        """
        result = transform_weather_records(sample_weather_records)

        assert result["status"] == "success"
        assert len(result["weather_records"]) == 1

        transformed_record = result["weather_records"][0]
        assert transformed_record["city"] == "New York"
        assert transformed_record["country"] == "US"
        assert isinstance(transformed_record["date_time"], datetime.datetime)
        assert transformed_record["weather"] == "Clouds"
        assert transformed_record["description"] == "broken clouds"

    def test_invalid_input_type(self) -> None:
        """
        Test for invalid input type to the transform_weather_records function

        Returns:
            None
        """
        invalid_input = "not a list"
        result = transform_weather_records(invalid_input)

        assert result["status"] == "error"
        assert "Invalid input type" in result["message"]

    def test_missing_required_fields(self) -> None:
        """
        Test for missing required fields in the input data to transform_weather_records

        Returns:
            None
        """
        incomplete_record = [{"city": "New York", "country": "US"}]

        result = transform_weather_records(incomplete_record)
        assert result["status"] == "error"
        assert "Unable to transform weather records" in result["message"]

    def test_empty_list_input(self) -> None:
        """
        Test for empty list input to the transform_weather_records function

        Returns:
            None
        """
        result = transform_weather_records([])
        assert result["status"] == "success"
        assert len(result["weather_records"]) == 0

    def test_multiple_records(self) -> None:
        """
        Test for multiple records in the input data to transform_weather_records

        Returns:
            None
        """
        multiple_records = [
            {
                "city": "New York",
                "country": "US",
                "state": "NY",
                "lat": 40.7128,
                "lon": -74.0060,
                "timezone": "America/New_York",
                "timezone_offset": -14400,
                "current": {
                    "dt": 1635724800,
                    "sunrise": 1635766800,
                    "sunset": 1635803400,
                    "temp": 15.5,
                    "feels_like": 14.8,
                    "pressure": 1015,
                    "humidity": 76,
                    "dew_point": 11.2,
                    "uvi": 4.5,
                    "clouds": 75,
                    "visibility": 10000,
                    "wind_speed": 3.6,
                    "wind_deg": 180,
                    "weather": [{"main": "Clouds", "description": "broken clouds"}],
                },
            }
        ] * 2

        result = transform_weather_records(multiple_records)
        assert result["status"] == "success"
        assert len(result["weather_records"]) == 2


class TestLoadRecordsToLocationDim:
    @pytest.fixture
    def mock_bigquery_client(self):
        with patch("google.cloud.bigquery.Client") as mock_client:
            yield mock_client

    @pytest.fixture
    def mock_hash_function_location(self, sample_weather_data) -> Dict[str, str]:
        """
        Mock hash function for the location dimension table

        Args:
            sample_weather_data (List[Dict]): Sample weather data records

        Returns:
            Dict[str, str]: Dictionary containing the hash key

        """

        def _hash_function(weather_data) -> Dict[str, str]:
            hash_dict = {}

            if weather_data["latitude"] and weather_data["longitude"]:
                hash_dict["hash_key"] = (
                    f"{weather_data['latitude']}_{weather_data['longitude']}"
                )
            else:
                hash_dict["hash_key"] = "location1"

            return hash_dict

        return _hash_function

    @pytest.fixture
    def mock_hash_function_fact(self) -> Dict[str, str]:
        """
        Mock hash function for the fact table

        Args:
            None

        Returns:
            Dict[str, str]: Dictionary containing the hash key
        """

        def _hash_function() -> Dict[str, str]:
            return {"hash_key": "fact1"}

        return _hash_function

    @patch("helpers.utils.create_table")
    @patch("helpers.utils.query_bigquery_existing_data")
    @patch("helpers.utils.insert_new_records")
    @patch("helpers.utils.insert_or_update_records_to_fact_table")
    def test_successful_load(
        self,
        mock_insert_or_update_records_to_fact_table,
        mock_insert_new_records,
        mock_query_bigquery_existing_data,
        mock_create_table,
        sample_weather_data,
        mock_hash_function_fact,
        mock_hash_function_location,
    ) -> None:
        """
        Test for successful loading of records to the location dimension table

        Args:
            mock_insert_or_update_records_to_fact_table (MagicMock): Mocked insert_or_update_records_to_fact_table pytest fixture
            mock_insert_new_records (MagicMock): Mocked insert_new_records pytest fixture
            mock_query_bigquery_existing_data (MagicMock): Mocked query_bigquery_existing_data pytest fixture
            mock_create_table (MagicMock): Mocked create_table pytest fixture
            sample_weather_data (List[Dict]): Sample weather data records
            mock_hash_function_fact (MagicMock): Mocked hash function for the fact table
            mock_hash_function_location (MagicMock): Mocked hash function for the location dimension table

        Returns:
            None
        """
        project_id = "weather-etl"
        dataset_name = "test_dataset"

        create_bq_dataset = create_dataset(project_id, dataset_name)
        dataset_id = create_bq_dataset["dataset_id"]

        mock_create_table.return_value = {
            "status": "success",
            "table_id": f"{dataset_id}.location_dim",
        }
        mock_query_bigquery_existing_data.return_value = {
            "body": {
                "existing_ids": [],
                "record_list": sample_weather_data,
            }
        }

        mock_insert_new_records.return_value = {"status": "success"}
        mock_insert_or_update_records_to_fact_table.return_value = {"status": "success"}

        location_table = "location_dim"
        fact_table = "weather_fact"

        result = load_records_to_location_dim(
            sample_weather_data,
            dataset_id,
            location_table,
            fact_table,
            mock_hash_function_location,
            mock_hash_function_fact,
        )

        assert result["status"] == "success"
        assert "location records have been loaded" in result["message"]

    def test_empty_weather_data(
        self, mock_hash_function_location, mock_hash_function_fact
    ) -> None:
        """
        Test for empty weather data in the load_records_to_location_dim function

        Args:
            mock_hash_function_location (MagicMock): Mocked hash function for the location dimension table
            mock_hash_function_fact (MagicMock): Mocked hash function for the fact table

        Returns:
            None
        """

        create_bq_dataset = create_dataset(project_id, dataset_name)
        dataset_id = create_bq_dataset["dataset_id"]

        location_table = "location_dim"
        fact_table = "weather_fact"
        sample_data = []
        result = load_records_to_location_dim(
            sample_data,
            dataset_id,
            location_table,
            fact_table,
            mock_hash_function_location,
            mock_hash_function_fact,
        )

        assert result["status"] == "success"
        assert "0 location records have been loaded" in result["message"]
