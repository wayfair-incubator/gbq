from typing import List

import pytest
from google.cloud.bigquery import SchemaField

from gbq.helpers import (
    _check_if_map,
    _map_raw_dictionary_to_bq_schema,
    get_bq_credentials,
    get_bq_schema_from_json,
    get_bq_schema_from_json_schema,
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
    shipment_schema = SchemaField("shipments", "RECORD", "REPEATED")
    shipment_nested_schema = [
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("warehouse_id", "INTEGER", "NULLABLE"),
    ]
    shipment_schema._fields = shipment_nested_schema
    expected = [
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("supplier_id", "INTEGER", "NULLABLE"),
        shipment_schema,
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
    pickable_dates_schema = SchemaField("pickable_dates", "RECORD", "REPEATED")
    pickable_dates_schema._fields = string_key_value_schema

    data_schema = SchemaField("data", "RECORD", "REPEATED")
    data_nested_schema = [
        SchemaField("warehouse_id", "INTEGER", "NULLABLE"),
        SchemaField("spr_id", "STRING", "NULLABLE"),
        pickable_dates_schema,
    ]
    data_schema._fields = data_nested_schema
    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        SchemaField("warehouses", "INTEGER", "REPEATED"),
        data_schema,
        SchemaField("requested_by", "STRING", "NULLABLE"),
    ]

    raw_input_data = {
        "request_id": 1,
        "warehouses": [5, 10, 11],
        "data": [
            {
                "warehouse_id": 1,
                "spr_id": "abc.123",
                "pickable_dates": [
                    {"key": "5", "value": "2020-05-11"},
                    {"key": "10", "value": "2020-05-11"},
                ],
            }
        ],
        "requested_by": "user_unknown",
    }

    response = _map_raw_dictionary_to_bq_schema(raw_input_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_list_of_int():
    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        SchemaField("warehouses", "INTEGER", "REPEATED"),
    ]

    raw_input_data = {
        "request_id": 2,
        "warehouses": [1, 2, 3],
    }

    response = get_bq_schema_from_json(raw_input_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_list_of_dicts():

    monthly_forecast_fields = [
        SchemaField("country_id", "INTEGER", "NULLABLE"),
        SchemaField("forecast", "FLOAT", "NULLABLE"),
        SchemaField("year_month", "STRING", "NULLABLE"),
    ]
    monthly_forecast_schema = SchemaField("monthly_forecast", "RECORD", "REPEATED")
    monthly_forecast_schema._fields = monthly_forecast_fields

    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        monthly_forecast_schema,
    ]

    raw_input_data = {
        "request_id": 2,
        "monthly_forecast": [
            {"country_id": 1, "forecast": 6.1167402267456055, "year_month": "2020 - 7"},
            {"country_id": 1, "forecast": 6.4262800216674805, "year_month": "2020 - 8"},
        ],
    }

    response = get_bq_schema_from_json(raw_input_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_empty_list():
    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        SchemaField("warehouses", "RECORD", "REPEATED"),
    ]

    raw_input_data = {
        "request_id": 2,
        "warehouses": [],
    }

    response = get_bq_schema_from_json(raw_input_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_maps(
    key_value_schema, string_key_value_schema
):
    eligible_warehouses_schema = SchemaField(
        "eligible_warehouses", "RECORD", "REPEATED"
    )
    eligible_warehouses_schema._fields = key_value_schema

    group_ids_schema = SchemaField("group_ids", "RECORD", "REPEATED")
    group_ids_schema._fields = string_key_value_schema

    monthly_forecast_fields = [
        SchemaField("country_id", "INTEGER", "NULLABLE"),
        SchemaField("forecast", "FLOAT", "NULLABLE"),
        SchemaField("year_month", "STRING", "NULLABLE"),
    ]
    monthly_forecast_schema = SchemaField("monthly_forecast", "RECORD", "REPEATED")
    monthly_forecast_schema._fields = monthly_forecast_fields

    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        eligible_warehouses_schema,
        group_ids_schema,
    ]

    raw_input_data = {
        "request_id": 2,
        "eligible_warehouses": {"5": 5, "10": 10, "11": 11},
        "group_ids": {"5": "5", "10": "10", "11": "11"},
    }

    response = get_bq_schema_from_json(raw_input_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_dict():
    monthly_forecast_fields = [
        SchemaField("country_id", "INTEGER", "NULLABLE"),
        SchemaField("forecast", "FLOAT", "NULLABLE"),
        SchemaField("year_month", "STRING", "NULLABLE"),
    ]
    monthly_forecast_schema = SchemaField("monthly_forecast", "RECORD", "NULLABLE")
    monthly_forecast_schema._fields = monthly_forecast_fields

    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        monthly_forecast_schema,
    ]

    raw_input_data = {
        "request_id": 2,
        "monthly_forecast": {
            "country_id": 1,
            "forecast": 6.1167402267456055,
            "year_month": "2020 - 7",
        },
    }

    response = get_bq_schema_from_json(raw_input_data)
    assert response == expected
