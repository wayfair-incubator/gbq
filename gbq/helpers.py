import datetime
import json
from collections import defaultdict

from google.cloud.bigquery import SchemaField
from google.oauth2 import service_account

from gbq.exceptions import GbqException, InvalidDefinitionException

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


def get_bq_credentials(credential: str) -> service_account.Credentials:
    """
    Function takes a stringified JSON Service Account and returns a Google Service Account object.

    Raises:
        GbqException: If the value is not valid service account JSON. The original error is
            deliberately not chained: json.JSONDecodeError keeps the whole input in its `doc`
            attribute, and error trackers that capture exception context would record the key.
    """
    try:
        info = json.loads(credential)
    except (TypeError, ValueError):
        raise GbqException("svc_account is not valid service account JSON") from None

    try:
        return service_account.Credentials.from_service_account_info(info)
    except (TypeError, ValueError, KeyError):
        raise GbqException(
            "svc_account JSON is not in the expected service account format"
        ) from None


def get_bq_schema_from_json_schema(source: list[dict]) -> list[SchemaField]:
    """
    Function coverts json table schema for a BQ table to a list of BQ SchemaField objects.

    Each field is built from its BigQuery API representation, so every attribute in the
    JSON is kept (e.g. policyTags, precision, scale, maxLength, defaultValueExpression,
    collation), and nested fields of RECORD and STRUCT columns are converted recursively.
    """
    return [SchemaField.from_api_repr(_with_field_defaults(field)) for field in source]


def _with_field_defaults(field: dict) -> dict:
    """
    Function returns a copy of a JSON schema field with gbq's defaults applied: mode
    NULLABLE, an empty description, and the same defaults for nested fields.
    """
    resource = dict(field)
    resource["name"] = str(resource.get("name"))
    resource["type"] = str(resource.get("type"))
    resource["mode"] = resource.get("mode") or "NULLABLE"
    if resource.get("description") is None:
        resource["description"] = ""
    if resource.get("fields"):
        resource["fields"] = [_with_field_defaults(f) for f in resource["fields"]]
    return resource


# Standard SQL type names the API reports under their legacy names
_TYPE_ALIASES = {
    "INT64": "INTEGER",
    "FLOAT64": "FLOAT",
    "BOOL": "BOOLEAN",
    "STRUCT": "RECORD",
}


def _normalize_schema_field(field: dict) -> dict:
    """
    Function normalizes an API representation of a schema field so that equivalent
    fields compare equal: an empty description equals none, types and modes are
    upper-cased with standard SQL aliases mapped, and mode defaults to NULLABLE.
    """
    normalized = dict(field)
    if not normalized.get("description"):
        normalized.pop("description", None)
    field_type = str(normalized.get("type", "")).upper()
    normalized["type"] = _TYPE_ALIASES.get(field_type, field_type)
    normalized["mode"] = str(normalized.get("mode") or "NULLABLE").upper()
    nested = [_normalize_schema_field(f) for f in normalized.pop("fields", None) or []]
    if nested:
        normalized["fields"] = nested
    return normalized


def schemas_equal(left: list[SchemaField], right: list[SchemaField]) -> bool:
    """
    Function returns whether two table schemas are equivalent. Any difference other
    than the normalizations in _normalize_schema_field makes them unequal.
    """
    return [_normalize_schema_field(f.to_api_repr()) for f in left] == [
        _normalize_schema_field(f.to_api_repr()) for f in right
    ]


def get_bq_schema_from_record(raw_data: dict) -> list[SchemaField]:
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
    return all(key.isdigit() for key in keys)


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


def _map_raw_dictionary_to_bq_schema(raw_data: dict) -> list[SchemaField]:
    """
    Function loops over a dictionary of raw data and returns a BQ Table schema object.
    """
    # SchemaField list
    schema: list[SchemaField] = []

    # Iterate the existing dictionary
    for key, value in raw_data.items():
        nested_fields: tuple = ()
        mode = "NULLABLE"  # Default mode

        # Determine if we need nested fields
        if value and (
            isinstance(value, dict)
            or (isinstance(value, list) and value and isinstance(value[0], dict))
        ):
            field_type_str = "RECORD"
            if isinstance(value, dict):
                nested_fields = tuple(_map_raw_dictionary_to_bq_schema(value))
            else:
                nested_fields = tuple(_map_raw_dictionary_to_bq_schema(value[0]))
                mode = "REPEATED"  # Lists of dicts should be REPEATED
        else:
            field_type_str = None

        try:
            # NULLABLE By Default
            if field_type_str:
                schema_field = SchemaField(
                    key, field_type_str, mode=mode, fields=nested_fields
                )
            else:
                schema_field = SchemaField(key, field_type[type(value)])
        except KeyError:
            schema_field = _handle_exception(key, value)

        # Add the field to the list of fields
        schema.append(schema_field)

    # Return the dictionary values
    return schema


def _handle_exception(key, value) -> SchemaField:
    """
    Function builds a SchemaField for values whose type is not in field_type:
        nulls, lists and tuples. Raises InvalidDefinitionException for anything else.
    """
    if value is None:
        # A null carries no type information; STRING is the most permissive choice.
        return SchemaField(key, "STRING")

    if isinstance(value, list | tuple):
        if not value:
            # Managing empty list case
            return SchemaField(key, "RECORD", mode="REPEATED")
        if type(value[0]) in field_type:
            return SchemaField(key, field_type[type(value[0])], mode="REPEATED")

    raise InvalidDefinitionException(
        f"Cannot infer a BigQuery type for field '{key}' "
        f"with value of type {type(value).__name__}"
    )
