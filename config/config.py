import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from airflow.models import Variable
from typing import Dict, List, Union
from config.logger_config import error_log_file_path, log_file_path

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

error_logger = logging.getLogger("error_logger")
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler(error_log_file_path)
error_logger.addHandler(error_handler)

logger_handler = logging.FileHandler(log_file_path)
logger = logging.getLogger("logger")
logger.addHandler(logger_handler)

API_KEY = os.getenv("OPEN_WEATHER_API_KEY")
COUNTRY_CITY_API_KEY = os.getenv("COUNTRY_CITY_API_KEY") + "=="
CITY_NAMES = os.getenv("CITY_NAMES")
FIELDS = os.getenv("FIELDS")
WEATHER_FIELDS_EXCLUDE = os.getenv("WEATHER_FIELDS_EXCLUDE")
DATABASE_URL = os.getenv("DATABASE_URL")
COUNTRY_NAMES = os.getenv("COUNTRY_NAMES")
START_DATE_YEAR = os.getenv("START_DATE_YEAR")
END_DATE_YEAR = os.getenv("END_DATE_YEAR")
GCP_PROJECT_ID = os.getenv("GOOGLE__CLOUD__PROJECT__ID")
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
FACT_TABLE_NAME = os.getenv("BIGQUERY_TABLE_WEATHER_FACT")
LOCATION_TABLE_NAME = os.getenv("BIGQUERY_TABLE_LOCATION")
DATE_TABLE_NAME = os.getenv("BIGQUERY_TABLE_DATE")
WEATHER_TYPE_TABLE_NAME = os.getenv("BIGQUERY_TABLE_WEATHER_TYPE")
AIRFLOW_API_KEY = Variable.get("API_KEY", default_var=API_KEY)
AIRFLOW_CITY_NAMES = Variable.get("CITY_NAMES", default_var=CITY_NAMES)
AIRFLOW_COUNTRY_NAMES = Variable.get("COUNTRY_NAMES", default_var=COUNTRY_NAMES)
AIRFLOW_FIELDS = Variable.get("FIELDS", default_var=FIELDS)
AIRFLOW_WEATHER_FIELDS_EXCLUDE = Variable.get(
    "WEATHER_FIELDS_EXCLUDE", default_var=WEATHER_FIELDS_EXCLUDE
)
AIRFLOW_START_DATE_YEAR = Variable.get("START_DATE_YEAR", default_var=START_DATE_YEAR)
AIRFLOW_END_DATE_YEAR = Variable.get("END_DATE_YEAR", default_var=END_DATE_YEAR)
AIRFLOW_COUNTRY_CITY_API_KEY = Variable.get(
    "COUNTRY_CITY_API_KEY", default_var=COUNTRY_CITY_API_KEY
)


def process_var(var: str) -> Dict[str, Union[str, List[str]]]:
    """
    Process the environmental variable from a string to a list of strings if
    there is a comma in the varable

    Args: var(str) An environmental variable.

    returns: A dictionary containing the status, message and the processed variable

    Example: {
        "status": "success",
        "message": "Variable processed successfully",
        "processed_variable": ["lagos", "ibadan", "kano", "accra"]
    }
    """
    try:
        if isinstance(var, str) is True:
            if "," in var:
                split_var = var.split(",")
                strp_var = [city.lower().strip() for city in split_var]
                return {
                    "status": "success",
                    "message": "Variable processed successfully",
                    "processed_variable": strp_var,
                }

            else:
                strp_var = [var.lower().strip()]
                return {
                    "status": "success",
                    "message": "Variable processed successfully",
                    "processed_variable": strp_var,
                }
        else:
            error_logger.error(
                {
                    "status": "error",
                    "error": "Invalid input. Please provide a string as argument",
                }
            )
            return {
                "status": "error",
                "error": "Invalid input. Please provide a string as argument",
            }
    except Exception as e:
        error_logger.error(
            {
                "status": "error",
                "error": f"An error occured while processing the variable {var}: {e}",
            }
        )
        return {
            "status": "error",
            "error": f"An error occured while processing the variable  {var}: {e}",
        }


CITY_NAMES = process_var(CITY_NAMES)["processed_variable"]
FIELDS = process_var(FIELDS)["processed_variable"]
COUNTRY_NAMES = process_var(COUNTRY_NAMES)["processed_variable"]

AIRFLOW_CITY_NAMES = process_var(AIRFLOW_CITY_NAMES)["processed_variable"]
AIRFLOW_FIELDS = process_var(AIRFLOW_FIELDS)["processed_variable"]
AIRFLOW_COUNTRY_NAMES = process_var(AIRFLOW_COUNTRY_NAMES)["processed_variable"]
