from decimal import Decimal

import pytest
from google.cloud.bigquery import SchemaField

from gbq.exceptions import GbqException, InvalidDefinitionException
from gbq.helpers import (
    _check_if_map,
    _map_raw_dictionary_to_bq_schema,
    get_bq_credentials,
    get_bq_schema_from_json_schema,
    get_bq_schema_from_record,
    schemas_equal,
)


@pytest.fixture()
def key_value_schema() -> tuple:
    return (
        SchemaField("key", "STRING", "NULLABLE"),
        SchemaField("value", "INTEGER", "NULLABLE"),
    )


@pytest.fixture()
def string_key_value_schema() -> tuple:
    return (
        SchemaField("key", "STRING", "NULLABLE"),
        SchemaField("value", "STRING", "NULLABLE"),
    )


def test_get_bq_credentials(mocker):
    expected = '{"secret": "secret"}'
    mocker.patch(
        "gbq.helpers.service_account.Credentials.from_service_account_info"
    ).return_value = '{"secret": "secret"}'
    response = get_bq_credentials('{"secret": "secret"}')
    assert response == expected


def test_convert_to_bq_schema(nested_json_schema):
    address_nested_schema = [
        SchemaField("id", "INTEGER", "NULLABLE", description="ID DESCRIPTION"),
        SchemaField("street", "STRING", "NULLABLE", description="STREET DESCRIPTION"),
    ]
    address_schema = SchemaField(
        "address",
        "RECORD",
        "REPEATED",
        description="ADDRESS DESCRIPTION",
        fields=tuple(address_nested_schema),
    )
    expected = [
        SchemaField("id", "INTEGER", "NULLABLE", description=""),
        SchemaField("username", "STRING", "NULLABLE", description=""),
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
    dates_schema = SchemaField(
        "dates", "RECORD", "REPEATED", fields=string_key_value_schema
    )

    data_nested_schema = (
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("street", "STRING", "NULLABLE"),
        dates_schema,
    )
    data_schema = SchemaField("data", "RECORD", "REPEATED", fields=data_nested_schema)
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
    nested_fields = (
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("value", "FLOAT", "NULLABLE"),
        SchemaField("comment", "STRING", "NULLABLE"),
    )
    some_data_schema = SchemaField(
        "some_data", "RECORD", "REPEATED", fields=nested_fields
    )

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
    repeated_record_schema = SchemaField(
        "repeated_record", "RECORD", "REPEATED", fields=key_value_schema
    )
    some_ids_schema = SchemaField(
        "some_ids", "RECORD", "REPEATED", fields=string_key_value_schema
    )

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
    some_data_fields = (
        SchemaField("id", "INTEGER", "NULLABLE"),
        SchemaField("value", "FLOAT", "NULLABLE"),
        SchemaField("comment", "STRING", "NULLABLE"),
    )
    some_data_schema = SchemaField(
        "some_data", "RECORD", "NULLABLE", fields=some_data_fields
    )

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


def test_get_bq_schema_from_raw_data_dictionary_none_value():
    expected = [
        SchemaField("request_id", "INTEGER", "NULLABLE"),
        SchemaField("comment", "STRING", "NULLABLE"),
    ]

    raw_data = {"request_id": 2, "comment": None}

    response = get_bq_schema_from_record(raw_data)
    assert response == expected


def test_get_bq_schema_from_raw_data_dictionary_only_none_value():
    response = get_bq_schema_from_record({"comment": None})
    assert response == [SchemaField("comment", "STRING", "NULLABLE")]


def test_get_bq_schema_from_raw_data_dictionary_tuple_of_int():
    response = get_bq_schema_from_record({"numbers": (1, 2, 3)})
    assert response == [SchemaField("numbers", "INTEGER", "REPEATED")]


@pytest.mark.parametrize("value", [Decimal("1.5"), object(), [object()]])
def test_get_bq_schema_from_raw_data_dictionary_unsupported_type(value):
    with pytest.raises(InvalidDefinitionException, match="field 'amount'"):
        get_bq_schema_from_record({"request_id": 2, "amount": value})


@pytest.mark.parametrize("value", ["not json", None, '{"private_key": "abc"'])
def test_get_bq_credentials_invalid_json_does_not_leak_value(value):
    with pytest.raises(
        GbqException, match="not valid service account JSON"
    ) as exc_info:
        get_bq_credentials(value)

    # The JSON error (which holds the full input in .doc) must not be chained
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__
    if value:
        assert value not in str(exc_info.value)


def test_get_bq_credentials_wrong_format():
    key_json = '{"type": "service_account", "private_key": "SECRET"}'

    with pytest.raises(
        GbqException, match="expected service account format"
    ) as exc_info:
        get_bq_credentials(key_json)

    assert exc_info.value.__suppress_context__
    assert "SECRET" not in str(exc_info.value)


def test_schemas_equal_ignores_api_differences():
    ours = get_bq_schema_from_json_schema(
        [
            {"name": "id", "type": "integer"},
            {
                "name": "address",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [{"name": "street", "type": "STRING"}],
            },
        ]
    )
    from_api = [
        SchemaField.from_api_repr({"name": "id", "type": "INT64", "mode": "NULLABLE"}),
        SchemaField.from_api_repr(
            {
                "name": "address",
                "type": "STRUCT",
                "mode": "REPEATED",
                "fields": [{"name": "street", "type": "STRING"}],
            }
        ),
    ]

    assert schemas_equal(ours, from_api)


@pytest.mark.parametrize(
    "other",
    [
        [SchemaField("id", "STRING")],
        [SchemaField("id", "INTEGER", mode="REQUIRED")],
        [SchemaField("id", "INTEGER", description="new")],
        [SchemaField("id", "INTEGER"), SchemaField("name", "STRING")],
        [],
    ],
)
def test_schemas_equal_detects_changes(other):
    assert not schemas_equal([SchemaField("id", "INTEGER")], other)


def test_schemas_equal_is_order_sensitive():
    a, b = SchemaField("a", "STRING"), SchemaField("b", "STRING")
    assert not schemas_equal([a, b], [b, a])


def test_convert_to_bq_schema_keeps_field_attributes():
    schema = get_bq_schema_from_json_schema(
        [
            {
                "name": "amount",
                "type": "NUMERIC",
                "precision": "10",
                "scale": "2",
                "defaultValueExpression": "0",
                "policyTags": {
                    "names": ["projects/p/locations/us/taxonomies/1/tags/2"]
                },
            },
            {
                "name": "address",
                "type": "STRUCT",
                "fields": [{"name": "street", "type": "STRING", "maxLength": "50"}],
            },
        ]
    )

    amount, address = schema
    assert (amount.precision, amount.scale) == (10, 2)
    assert amount.default_value_expression == "0"
    assert amount.policy_tags.names == ("projects/p/locations/us/taxonomies/1/tags/2",)
    assert amount.mode == "NULLABLE"
    assert amount.description == ""
    # STRUCT fields are nested like RECORD fields, with the same defaults
    (street,) = address.fields
    assert street.max_length == 50
    assert (street.mode, street.description) == ("NULLABLE", "")


def test_convert_to_bq_schema_does_not_mutate_input():
    source = [
        {"name": "a", "type": "RECORD", "fields": [{"name": "b", "type": "INT64"}]}
    ]
    get_bq_schema_from_json_schema(source)
    assert source == [
        {"name": "a", "type": "RECORD", "fields": [{"name": "b", "type": "INT64"}]}
    ]
