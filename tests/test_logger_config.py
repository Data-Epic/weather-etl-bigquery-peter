import os

import pytest

from config.logger_config import create_log_file


curr_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(curr_dir)
var_dir = os.path.join(base_dir, "var")

if not os.path.exists(var_dir):
    os.makedirs(var_dir)


def test_create_log_file() -> None:
    """
    Test to create a log file with valid input arguments

    Returns: None

    """

    log_file_name = "test.log"
    error_log_file_name = "test_error.log"

    log_file_path, error_log_file_path = create_log_file(
        log_file_name, error_log_file_name, var_dir
    )

    assert os.path.exists(log_file_path)
    assert os.path.exists(error_log_file_path)


def test_create_log_file_invalid_file_format() -> None:
    """
    Test to create a log file with invalid file format

    Returns: None

    """

    log_file_name = "test.txt"
    error_log_file_name = "test_error.txt"

    with pytest.raises(ValueError) as e:
        create_log_file(log_file_name, error_log_file_name, var_dir)

    assert str(e.value) == "Invalid file format. Only log files are allowed"


def test_create_log_file_invalid_input_arguments() -> None:
    """
    Test to create a log file with invalid input arguments

    Returns: None

    """

    log_file_name = 123
    error_log_file_name = 123

    with pytest.raises(ValueError) as e:
        create_log_file(log_file_name, error_log_file_name, var_dir)

    assert str(e.value) == "Invalid input arguments. Input arguments must be strings"
