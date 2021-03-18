import datetime
import json
from collections import defaultdict
from typing import Dict, List

from google.cloud.bigquery import SchemaField
from google.oauth2 import service_account

field_type = {
    str: "STRING",
    bytes: "BYTES",
    int: "INTEGER",
    float: "FLOAT",
    bool: "BOOLEAN",
    datetime.datetime: "DATETIME",
    datetime.date: "DATE",
    datetime.time: "TIME",
    dict: "RECORD",
    defaultdict: "RECORD",
}


def get_bq_credentials(credential: str):
    """
    Function takes a stringified JSON Service Account and returns a Google Service Account object.
    """
    return service_account.Credentials.from_service_account_info(json.loads(credential))


def get_bq_schema_from_json_schema(source: List[Dict]) -> List[SchemaField]:
    """
    Function coverts json table schema for a BQ table to a list of BQ SchemaField objects.
    """
    # SchemaField list
    schema: List[SchemaField] = []

    for key, value in enumerate(source):
        schema_field = SchemaField(
            value.get("name"),
            value.get("type"),
            value.get("mode", "NULLABLE"),
            value.get("description", None),
        )

        # Add the field to the list of fields
        schema.append(schema_field)

        # If it is a STRUCT / RECORD field we start the recursion
        if schema_field.field_type == "RECORD":
            schema_field._fields = get_bq_schema_from_json_schema(value.get("fields", []))  # type: ignore

    return schema


def get_bq_schema_from_record(raw_data: dict) -> List[SchemaField]:
    """
    Function builds a BQ Table schema from raw data of the table.
    """
    flat_data = _flatten_data(raw_data)
    return _map_raw_dictionary_to_bq_schema(flat_data)


def _check_if_map(value: dict):
    """
    BQ is confused when we pass it a map<int> or map<str> type, we should flatten
        such dictionaries as List of {"key": key, "value": value}.
    This function returns a boolean value,
        based on whether the dictionary in question is a map or not.
    """
    keys = value.keys()
    for key in keys:
        if not key.isdigit():
            return False
    return True


# flake8: noqa: C901
def _flatten_data(data: dict):
    """
    Function flattens the data.
    """
    flat_data: dict = defaultdict()

    for key in data:
        if isinstance(data[key], dict):
            _handle_dictionary(data, flat_data, key)

        elif isinstance(data[key], list):
            _handle_list(data, flat_data, key)

        else:
            flat_data[key] = data[key]

    return flat_data


def _handle_list(data, flat_data, key):
    if data[key]:
        list_data = []
        for list_item in data[key]:
            if isinstance(list_item, dict):
                list_data.append(_flatten_data(list_item))
            else:
                list_data.append(list_item)
        flat_data[key] = list_data
    else:
        flat_data[key] = data[key]


def _handle_dictionary(data, flat_data, key):
    if not _check_if_map(data[key]):
        flat_data[key] = data[key]
    else:
        flat_data[key] = [
            {"key": key, "value": value} for key, value in data[key].items()
        ]


def _map_raw_dictionary_to_bq_schema(raw_data: dict) -> List[SchemaField]:
    """
    Function loops over a dictionary of raw dara and returns a BQ Table schema object.
    """
    # SchemaField list
    schema: List[SchemaField] = []

    # Iterate the existing dictionary
    for key, value in raw_data.items():

        try:
            # NULLABLE By Default
            schema_field = SchemaField(key, field_type[type(value)])
        except KeyError:
            schema_field = _handle_exception(key, schema_field, value)

        # Add the field to the list of fields
        schema.append(schema_field)

        # If it is a STRUCT / RECORD field we start the recursion
        if schema_field.field_type == "RECORD" and value:
            if isinstance(value, dict):
                schema_field._fields = _map_raw_dictionary_to_bq_schema(value)
            else:
                schema_field._fields = _map_raw_dictionary_to_bq_schema(value[0])

    # Return the dictionary values
    return schema


def _handle_exception(key, schema_field, value):
    # We are expecting a REPEATED field
    if value and len(value) > 0:
        schema_field = SchemaField(
            key, field_type[type(value[0])], mode="REPEATED"
        )  # REPEATED
    elif isinstance(value, list) and not value:
        # Managing empty list case
        schema_field = SchemaField(key, "RECORD", mode="REPEATED")  # REPEATED
    return schema_field
