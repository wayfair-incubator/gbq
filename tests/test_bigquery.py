import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, bigquery_v2
from google.cloud.bigquery import PartitionRange
from google.cloud.bigquery.routine import RoutineArgument
from pydantic import ValidationError

from gbq.bigquery import BigQuery
from gbq.dto import (
    Partition,
    RangeDefinition,
    RangeFieldDefinition,
    Structure,
    TimeDefinition,
)
from tests.fixtures import (
    Routine,
    Table,
    empty_arguments_structure,
    raw_routine,
    raw_table,
    structure_with_arguments,
)


@pytest.fixture()
def bq(mocker) -> BigQuery:
    mocker.patch(
        "gbq.helpers.service_account.Credentials.from_service_account_info"
    ).return_value = '{"secret": "secret"}'
    mocker.patch("gbq.bigquery.bigquery.Client")
    return BigQuery('{"secret": "secret"}', "project")


@pytest.fixture()
def table(mocker) -> Table:
    table = Table(
        project="project",
        dataset_id="dataset",
        table_id="structure",
        table_type="TABLE",
    )
    mocker.patch(
        "gbq.helpers.service_account.Credentials.from_service_account_info"
    ).return_value = '{"secret": "secret"}'
    mocker.patch("gbq.bigquery.BigQuery._add_partitioning_scheme")
    return table


def test_create_bigquery_class(mocker):
    mocker.patch(
        "gbq.helpers.service_account.Credentials.from_service_account_info"
    ).return_value = '{"secret": "secret"}'
    mocker.patch("gbq.bigquery.bigquery.Client")
    BigQuery('{"secret": "secret"}', "project")
    assert True


def test_get_dataset_in_project(bq, dataset_list_items):
    bq.bq_client.list_datasets.return_value = dataset_list_items
    response = bq.get_dataset_in_project("project")
    assert response == dataset_list_items


def test_get_table_in_project(bq, dataset_list_items, tables):
    bq.bq_client.list_datasets.return_value = dataset_list_items
    bq.bq_client.list_tables.return_value = tables
    response = bq.get_table_in_project("project")
    assert response == tables + tables


def test_get_structure(bq, table_with_schema):
    bq.bq_client.get_table.return_value = table_with_schema
    response = bq.get_structure("project", "dataset", "structure")
    assert response == table_with_schema


@pytest.mark.parametrize(
    "mocked_structure, expected",
    [
        (raw_routine(), Routine),
        (raw_table(), Table),
    ],
)
def test_create_or_update_structure_response(mocker, bq, mocked_structure, expected):
    mocker.patch("gbq.bigquery.BigQuery._handle_stored_procedure")
    mocker.patch("gbq.bigquery.BigQuery._handle_table_or_view")
    bq._handle_stored_procedure.return_value = Routine(
        routine_id="project.dataset.structure",
        type_="PROCEDURE",
        language="SQL",
        body="SELECT * FROM table",
    )
    bq._handle_table_or_view.return_value = Table(
        project="project1",
        dataset_id="abc",
        table_id="123",
        table_type="TABLE",
    )
    response = bq.create_or_update_structure(
        "project", "dataset", "structure", mocked_structure
    )
    assert isinstance(response, expected)


def test_get_routine(bq, routine):
    bq.bq_client.get_routine.return_value = routine
    response = bq.get_routine("project", "dataset", "structure")
    assert response == routine


def test_create_or_update_structure_with_no_partition(bq, nested_json_schema, table):
    bq.create_or_update_structure("project", "dataset", "structure", nested_json_schema)
    bq._add_partitioning_scheme.assert_not_called()
    assert True


def test_create_or_update_structure_with_default_time_partition(
    bq, nested_json_schema_with_time_partition, table
):
    bq.bq_client.get_table.side_effect = NotFound("")
    bq.create_or_update_structure(
        "project", "dataset", "structure", nested_json_schema_with_time_partition
    )

    bq._add_partitioning_scheme.assert_called_once()
    assert True


def test_create_or_update_structure_with_incorrect_partition(
    bq, nested_json_schema_with_incorrect_partition, table
):
    bq.bq_client.get_table.side_effect = NotFound("")
    with pytest.raises(ValidationError):
        bq.create_or_update_structure(
            "project",
            "dataset",
            "structure",
            nested_json_schema_with_incorrect_partition,
        )


def test_create_table_with_clustering(bq, nested_json_schema_with_clustering, table):
    bq.bq_client.get_table.side_effect = NotFound("")
    table = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema_with_clustering,
    )

    assert table.clustering_fields == nested_json_schema_with_clustering["clustering"]


def test_create_table_without_clustering(bq, nested_json_schema, table):
    bq.bq_client.get_table.side_effect = NotFound("")
    table = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema,
    )

    assert not table.clustering_fields


def test_update_table_with_clustering(bq, nested_json_schema_with_clustering, table):
    table = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema_with_clustering,
    )

    assert table.clustering_fields == nested_json_schema_with_clustering["clustering"]


def test_update_table_without_clustering(mocker, bq, nested_json_schema, table):
    mocker.patch(
        "gbq.bigquery.BigQuery.create_or_update_structure"
    ).return_value = table
    response_table = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema,
    )

    assert not response_table.clustering_fields


def test_create_table_with_labels(bq, nested_json_schema_with_labels, table):
    structure = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema_with_labels,
    )

    assert structure.labels == nested_json_schema_with_labels["labels"]


def test_create_table_without_labels(mocker, bq, nested_json_schema, table):
    mocker.patch(
        "gbq.bigquery.BigQuery.create_or_update_structure"
    ).return_value = table
    table = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema,
    )

    assert not table.labels


def test_update_table_with_same_labels(bq, nested_json_schema_with_labels, table):
    table = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema_with_labels,
    )

    assert table.labels == nested_json_schema_with_labels["labels"]


def test_update_table_without_labels(bq, nested_json_schema_with_labels, table):
    structure = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema_with_labels,
    )

    assert structure.labels == nested_json_schema_with_labels["labels"]


def test_create_table_with_description(bq, nested_json_schema_with_description, table):
    structure = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema_with_description,
    )

    assert structure.description == nested_json_schema_with_description["description"]


def test_create_table_without_description(mocker, bq, nested_json_schema, table):
    mocker.patch(
        "gbq.bigquery.BigQuery.create_or_update_structure"
    ).return_value = table
    table = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema,
    )

    assert not table.description


def test_update_table_with_same_description(
    bq, nested_json_schema_with_description, table
):
    table = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema_with_description,
    )

    assert table.description == nested_json_schema_with_description["description"]


def test_update_table_without_description(
    bq, nested_json_schema_with_description, table
):
    response_table = bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        nested_json_schema_with_description,
    )

    assert (
        response_table.description == nested_json_schema_with_description["description"]
    )


def test__get_table_schema(bq, nested_json_schema):
    expected = Structure(**{"schema": nested_json_schema})
    assert bq._get_structure(nested_json_schema) == expected


def test__get_table_schema_with_partition(bq, nested_json_schema_with_time_partition):
    expected = Structure(**nested_json_schema_with_time_partition)
    assert bq._get_structure(nested_json_schema_with_time_partition) == expected


def test__add_partitioning_scheme_time(bq, table_with_schema):
    time_definition = TimeDefinition(type="DAY")
    partition_scheme = Partition(type="time", definition=time_definition)

    expected = bigquery.TimePartitioning(type_="DAY")
    bq_structure = bq._add_partitioning_scheme(
        bq_structure=table_with_schema, partition_scheme=partition_scheme
    )

    assert not bq_structure.range_partitioning
    assert bq_structure.time_partitioning == expected


def test__add_partitioning_scheme_time_with_field_and_expiration(bq, table_with_schema):
    time_definition = TimeDefinition(type="DAY", expirationMs="1", field="id")
    partition_scheme = Partition(type="time", definition=time_definition)

    expected = bigquery.TimePartitioning(type_="DAY", field="id", expiration_ms="1")
    bq_structure = bq._add_partitioning_scheme(
        bq_structure=table_with_schema, partition_scheme=partition_scheme
    )

    assert not bq_structure.range_partitioning
    assert bq_structure.time_partitioning == expected


def test__add_partitioning_scheme_range(bq, table_with_schema):
    range_field_definition = RangeFieldDefinition(start=1, end=100000, interval=10)
    range_definition = RangeDefinition(field="id", range=range_field_definition)
    partition_scheme = Partition(type="range", definition=range_definition)

    expected = bigquery.RangePartitioning(field=range_definition.field)
    expected.range_ = PartitionRange(**range_definition.range.__dict__)
    bq_structure = bq._add_partitioning_scheme(
        bq_structure=table_with_schema, partition_scheme=partition_scheme
    )

    assert not bq_structure.time_partitioning
    assert bq_structure.range_partitioning.field == expected.field
    assert bq_structure.range_partitioning.range_.start == expected.range_.start
    assert bq_structure.range_partitioning.range_.end == expected.range_.end
    assert bq_structure.range_partitioning.range_.interval == expected.range_.interval


def test_update_view(bq, sql_schema):
    bq.create_or_update_view("project", "dataset", "structure", sql_schema)
    bq.bq_client.update_table.assert_called_once()


def test_create_view(bq, sql_schema):
    bq.bq_client.get_table.side_effect = NotFound("")
    bq.create_or_update_view("project", "dataset", "structure", sql_schema)
    bq.bq_client.create_table.assert_called_once()


def test__handle_create_structure_table_without_partition_and_clustering(
    bq, table_structure
):
    bq_structure = bq._handle_create_structure(
        dataset="dataset",
        project="project",
        structure_id="structure",
        structure=table_structure,
    )
    assert not bq_structure.clustering_fields
    assert not bq_structure.time_partitioning
    assert not bq_structure.range_partitioning


def test__handle_create_structure_table_with_partition_and_clustering(
    bq, table_structure_with_clustering_partition
):
    bq_structure = bq._handle_create_structure(
        dataset="dataset",
        project="project",
        structure_id="structure",
        structure=table_structure_with_clustering_partition,
    )

    expected_time_partition = bigquery.TimePartitioning(type_="DAY")

    assert bq_structure.clustering_fields == ["id"]
    assert bq_structure.time_partitioning == expected_time_partition
    assert not bq_structure.range_partitioning


def test__handle_create_structure_view(bq, view_structure):
    bq_structure = bq._handle_create_structure(
        dataset="dataset",
        project="project",
        structure_id="structure",
        structure=view_structure,
    )
    assert not bq_structure.clustering_fields
    assert not bq_structure.time_partitioning
    assert not bq_structure.range_partitioning
    assert not bq_structure.schema
    assert not bq_structure.labels
    assert not bq_structure.description
    assert bq_structure.view_query == view_structure.view_query


def test__handle_create_structure_view_with_label_and_description(
    bq, view_structure_with_labels_and_description
):
    bq_structure = bq._handle_create_structure(
        dataset="dataset",
        project="project",
        structure_id="structure",
        structure=view_structure_with_labels_and_description,
    )
    assert not bq_structure.clustering_fields
    assert not bq_structure.time_partitioning
    assert not bq_structure.range_partitioning
    assert not bq_structure.schema
    assert bq_structure.labels == view_structure_with_labels_and_description.labels
    assert (
        bq_structure.description
        == view_structure_with_labels_and_description.description
    )
    assert (
        bq_structure.view_query == view_structure_with_labels_and_description.view_query
    )


@pytest.mark.parametrize(
    "param, expected",
    [
        (empty_arguments_structure(), []),
        (
            structure_with_arguments(),
            [
                RoutineArgument(
                    name="x",
                    data_type=bigquery_v2.types.StandardSqlDataType(type_kind="DATE"),
                )
            ],
        ),
    ],
)
def test__handle_routine_arguments(bq, param, expected):
    response = bq._handle_routine_arguments(param)
    assert response == expected


def test__handle_stored_procedure_create(bq, routine_structure):
    bq.bq_client.get_routine.side_effect = NotFound("")
    bq._handle_stored_procedure("project", "dataset", "structure", routine_structure)
    bq.bq_client.create_routine.assert_called_once()


def test__handle_stored_procedure_update(bq, routine, routine_structure):
    bq.bq_client.get_routine.return_value = routine
    bq._handle_stored_procedure("project", "dataset", "structure", routine_structure)
    bq.bq_client.update_routine.assert_called_once()


def test_delete_dataset(bq):
    bq.bq_client.get_dataset.return_value = True
    response = bq.delete_dataset("project", "dataset")
    assert response


def test_delete_dataset_when_not_exists(bq):
    bq.bq_client.get_dataset.side_effect = NotFound("")
    response = bq.delete_dataset("project", "dataset")
    assert not response


def test_delete_table_or_view(bq):
    bq.bq_client.get_table.return_value = True
    response = bq.delete_table_or_view("project", "dataset", "table")
    assert response


def test_delete_table_or_view_when_not_exists(bq):
    bq.bq_client.get_table.side_effect = NotFound("")
    response = bq.delete_table_or_view("project", "dataset", "table")
    assert not response
