from typing import List, Optional

import pytest

from gbq.dto import Structure


class DatasetListItem:
    def __init__(self, dataset_id: str, project: str):
        self.dataset_id = dataset_id
        self.project = project


class SchemaField:
    def __init__(
        self, name: str, mode: str, field_type: str, fields=None, description=None
    ):
        if fields is None:
            fields = []
        self.name = name
        self.mode = mode
        self.field_type = field_type
        self.fields = fields
        self.description = description


class Table:
    def __init__(
        self,
        project: str,
        dataset_id: str,
        table_id: str,
        table_type: str,
        schema: Optional[List[SchemaField]] = None,
        view_query: str = None,
        clustering_fields: list = None,
        time_partitioning=None,
        range_partitioning=None,
        labels=None,
        description=None,
    ):
        self.project = project
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.table_type = table_type
        self.schema = schema
        self.view_query = view_query
        self.clustering_fields = clustering_fields
        self.time_partitioning = time_partitioning
        self.range_partitioning = range_partitioning
        self.labels = labels
        self.description = description


class Routine:
    def __init__(
        self,
        routine_id: str,
        type_: str,
        language: str,
        body: str,
    ):
        self.routine_id = routine_id
        self.type_ = type_
        self.language = language
        self.body = body


@pytest.fixture()
def nested_json_schema():
    return [
        {"name": "id", "mode": "NULLABLE", "type": "INTEGER"},
        {"name": "username", "mode": "NULLABLE", "type": "STRING"},
        {
            "fields": [
                {"name": "id", "mode": "NULLABLE", "type": "INTEGER"},
                {"name": "street", "mode": "NULLABLE", "type": "STRING"},
            ],
            "name": "address",
            "mode": "REPEATED",
            "type": "RECORD",
        },
    ]


@pytest.fixture()
def nested_json_schema_with_time_partition(nested_json_schema):
    return {
        "schema": nested_json_schema,
        "partition": {"type": "time", "definition": {"type": "DAY"}},
        "type": "table",
    }


@pytest.fixture()
def nested_json_schema_with_incorrect_partition(nested_json_schema):
    return {
        "schema": nested_json_schema,
        "partition": {"type": "time", "definition": {"type": "day"}},
        "type": "table",
    }


@pytest.fixture()
def nested_json_schema_with_clustering(nested_json_schema):
    return {
        "schema": nested_json_schema,
        "clustering": ["id"],
        "type": "table",
    }


@pytest.fixture()
def nested_json_schema_with_partition_and_clustering(nested_json_schema):
    return {
        "schema": nested_json_schema,
        "clustering": ["id"],
        "partition": {"type": "time", "definition": {"type": "DAY"}},
        "type": "table",
    }


@pytest.fixture()
def nested_json_schema_with_labels(nested_json_schema):
    return {
        "schema": nested_json_schema,
        "labels": {"test": "test"},
        "type": "table",
    }


@pytest.fixture()
def nested_json_schema_with_description(nested_json_schema):
    return {
        "schema": nested_json_schema,
        "description": "This is a table",
        "type": "table",
    }


@pytest.fixture()
def dataset_list_items() -> List[DatasetListItem]:
    return [
        DatasetListItem(dataset_id="abc", project="project"),
        DatasetListItem(dataset_id="def", project="project"),
    ]


@pytest.fixture()
def tables() -> List[Table]:
    return [
        Table(project="project", dataset_id="abc", table_id="123", table_type="TABLE"),
        Table(project="project", dataset_id="def", table_id="456", table_type="TABLE"),
    ]


@pytest.fixture()
def table_view() -> List[Table]:
    return [
        Table(project="project", dataset_id="abc", table_id="123", table_type="TABLE"),
        Table(project="project", dataset_id="def", table_id="456", table_type="VIEW"),
    ]


@pytest.fixture()
def views() -> List[Table]:
    return [
        Table(project="project", dataset_id="abc", table_id="123", table_type="VIEW"),
        Table(project="project", dataset_id="def", table_id="456", table_type="VIEW"),
    ]


@pytest.fixture()
def schema_fields() -> List[SchemaField]:
    return [
        SchemaField(name="id", mode="NULLABLE", field_type="INTEGER"),
        SchemaField(name="username", mode="NULLABLE", field_type="STRING"),
    ]


@pytest.fixture()
def nested_schema_fields() -> List[SchemaField]:
    nested_fields = [
        SchemaField(name="id", mode="NULLABLE", field_type="INTEGER"),
        SchemaField(name="username", mode="NULLABLE", field_type="STRING"),
    ]
    return [
        SchemaField(name="id", mode="NULLABLE", field_type="INTEGER"),
        SchemaField(name="street", mode="NULLABLE", field_type="STRING"),
        SchemaField(
            name="address", mode="REPEATED", field_type="RECORD", fields=nested_fields
        ),
    ]


@pytest.fixture()
def table_with_schema(schema_fields) -> Table:
    return Table(
        project="project1",
        dataset_id="abc",
        table_id="123",
        table_type="TABLE",
        schema=schema_fields,
    )


@pytest.fixture()
def sql_schema() -> str:
    return "SELECT * FROM table"


@pytest.fixture()
def table_structure(nested_json_schema) -> Structure:
    return Structure(
        **{
            "schema": nested_json_schema,
            "type": "table",
        }
    )


@pytest.fixture()
def table_structure_with_clustering_partition(
    nested_json_schema_with_partition_and_clustering,
) -> Structure:
    return Structure(**nested_json_schema_with_partition_and_clustering)


@pytest.fixture()
def view_structure(sql_schema) -> Structure:
    return Structure(**{"view_query": sql_schema, "type": "view"})


@pytest.fixture()
def view_structure_with_labels_and_description(sql_schema) -> Structure:
    return Structure(
        **{
            "view_query": sql_schema,
            "labels": {"test": "test"},
            "description": "test",
            "type": "view",
        }
    )


def raw_routine():
    return {"body": "SELECT * FROM table"}


def raw_table():
    return [
        {"name": "id", "mode": "NULLABLE", "type": "INTEGER"},
        {"name": "street", "mode": "NULLABLE", "type": "STRING"},
        {
            "fields": [
                {"name": "id", "mode": "NULLABLE", "type": "INTEGER"},
                {"name": "street", "mode": "NULLABLE", "type": "STRING"},
            ],
            "name": "address",
            "mode": "REPEATED",
            "type": "RECORD",
        },
    ]


@pytest.fixture()
def routine() -> Routine:
    return Routine(
        routine_id="project.dataset.structure",
        type_="PROCEDURE",
        language="SQL",
        body="SELECT * FROM table",
    )


@pytest.fixture()
def routine_structure() -> Structure:
    return empty_arguments_structure()


def empty_arguments_structure() -> Structure:
    return Structure(
        body="SELECT * FROM table",
    )


def structure_with_arguments() -> Structure:
    return Structure(
        body="SELECT * FROM table", arguments=[{"name": "x", "data_type": "date"}]
    )
