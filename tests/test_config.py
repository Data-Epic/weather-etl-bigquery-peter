from config.config import process_var


def test_process_var_single_value() -> None:
    """
    Test for a single value as an environment variable(No comma)

    Returns:
        None
    """
    assert process_var("Nigeria")["processed_variable"] == ["nigeria"]


def test_process_var_multiple_values() -> None:
    """
    Test for multiple values as an environment variable (with comma)

    Returns:
        None
    """
    assert process_var("Nigeria,USA")["processed_variable"] == ["nigeria", "usa"]


def test_process_var_multiple_values_with_invalid_input() -> None:
    """
    Test for multiple values as an environment variable (with comma)

    Returns:
        None
    """
    result_int = process_var(123)
    result_none = process_var(None)
    result_bool = process_var(True)
    result_float = process_var(123.45)

    assert result_int["status"] == "error"
    assert result_int["error"] == "Invalid input. Please provide a string as argument"
    assert result_none["status"] == "error"
    assert result_none["error"] == "Invalid input. Please provide a string as argument"
    assert result_bool["status"] == "error"
    assert result_bool["error"] == "Invalid input. Please provide a string as argument"
    assert result_float["status"] == "error"
    assert result_float["error"] == "Invalid input. Please provide a string as argument"
