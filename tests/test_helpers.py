from typing import List

import pytest
from google.cloud.bigquery import SchemaField

from gbq.helpers import (
    _check_if_map,
    _map_raw_dictionary_to_bq_schema,
    get_bq_credentials,
    get_bq_schema_from_json_schema,
    get_bq_schema_from_record,
)


@pytest.fixture()
def key_value_schema() -> List[SchemaField]:
    return [
        SchemaField("key", "STRING", "NULLABLE"),
        SchemaField("value", "INTEGER", "NULLABLE"),
    ]


@pytest.fixture()
def string_key_value_schema() -> List[SchemaField]:
    return [
        SchemaField("key", "STRING", "NULLABLE"),
        SchemaField("value", "STRING", "NULLABLE"),
    ]


def test_get_bq_credentials(mocker):
    expected = '{"secret": "secret"}'
    mocker.patch(
        "gbq.helpers.service_account.Credentials.from_service_account_info"
    ).return_value = '{"secret": "secret"}'
    response = get_bq_credentials('{"secret": "secret"}')
    assert response == expected


def test_convert_to_bq_schema(nested_json_schema):
    address_schema = SchemaField("address", "RECORD", "REPEATED")
    address_nested_schema = [
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("street", "STRING", "NULLABLE"),
    ]
    address_schema._fields = address_nested_schema
    expected = [
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("username", "STRING", "NULLABLE"),
        address_schema,
    ]

    response = get_bq_schema_from_json_schema(nested_json_schema)
    assert response == expected


def test__check_if_map_true():
    raw_input = {"5": 5, "10": 10, "11": 11}
    assert _check_if_map(raw_input)


def test__check_if_map_false():
    raw_input = {"request_id": 5, "requested_by": "[unknown]"}
    assert not _check_if_map(raw_input)


def test__map_raw_dictionary_to_bq_schema(string_key_value_schema):
    dates_schema = SchemaField("dates", "RECORD", "REPEATED")
    dates_schema._fields = string_key_value_schema

    data_schema = SchemaField("data", "RECORD", "REPEATED")
    data_nested_schema = [
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("street", "STRING", "NULLABLE"),
        dates_schema,
    ]
    data_schema._fields = data_nested_schema
    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        SchemaField("numbers", "INTEGER", "REPEATED"),
        data_schema,
        SchemaField("requested_by", "STRING", "NULLABLE"),
    ]

    raw_data = {
        "request_id": 1,
        "numbers": [5, 10, 11],
        "data": [
            {
                "id": 1,
                "street": "abc.123",
                "dates": [
                    {"key": "5", "value": "2020-05-11"},
                    {"key": "10", "value": "2020-05-11"},
                ],
            }
        ],
        "requested_by": "user_unknown",
    }

    response = _map_raw_dictionary_to_bq_schema(raw_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_list_of_int():
    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        SchemaField("numbers", "INTEGER", "REPEATED"),
    ]

    raw_input_data = {
        "request_id": 2,
        "numbers": [1, 2, 3],
    }

    response = get_bq_schema_from_record(raw_input_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_list_of_dicts():

    nested_fields = [
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("value", "FLOAT", "NULLABLE"),
        SchemaField("comment", "STRING", "NULLABLE"),
    ]
    some_data_schema = SchemaField("some_data", "RECORD", "REPEATED")
    some_data_schema._fields = nested_fields

    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        some_data_schema,
    ]

    raw_data = {
        "request_id": 2,
        "some_data": [
            {"id": 1, "value": 6.1167402267456055, "comment": "test"},
            {"id": 1, "value": 6.4262800216674805, "comment": "test"},
        ],
    }

    response = get_bq_schema_from_record(raw_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_empty_list():
    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        SchemaField("numbers", "RECORD", "REPEATED"),
    ]

    raw_data = {
        "request_id": 2,
        "numbers": [],
    }

    response = get_bq_schema_from_record(raw_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_maps(
    key_value_schema, string_key_value_schema
):
    repeated_record_schema = SchemaField("repeated_record", "RECORD", "REPEATED")
    repeated_record_schema._fields = key_value_schema

    some_ids_schema = SchemaField("some_ids", "RECORD", "REPEATED")
    some_ids_schema._fields = string_key_value_schema

    some_data = [
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("value", "FLOAT", "NULLABLE"),
        SchemaField("comment", "STRING", "NULLABLE"),
    ]
    some_data_schema = SchemaField("some_data", "RECORD", "REPEATED")
    some_data_schema._fields = some_data

    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        repeated_record_schema,
        some_ids_schema,
    ]

    raw_data = {
        "request_id": 2,
        "repeated_record": {"5": 5, "10": 10, "11": 11},
        "some_ids": {"5": "5", "10": "10", "11": "11"},
    }

    response = get_bq_schema_from_record(raw_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_dict():
    some_data_fields = [
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("value", "FLOAT", "NULLABLE"),
        SchemaField("comment", "STRING", "NULLABLE"),
    ]
    some_data_schema = SchemaField("some_data", "RECORD", "NULLABLE")
    some_data_schema._fields = some_data_fields

    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        some_data_schema,
    ]

    raw_data = {
        "request_id": 2,
        "some_data": {
            "id": 1,
            "value": 6.1167402267456055,
            "comment": "test",
        },
    }

    response = get_bq_schema_from_record(raw_data)
    assert response == expected
