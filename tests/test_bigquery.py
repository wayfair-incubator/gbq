import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import PartitionRange, QueryJob
from google.cloud.bigquery.routine import RoutineArgument
from pydantic import ValidationError

from gbq.bigquery import BigQuery
from gbq.dto import (
    Argument,
    BigQueryDataType,
    Partition,
    PartitionType,
    RangeDefinition,
    RangeFieldDefinition,
    Structure,
    StructureType,
    TimeDefinition,
    TimeType,
)
from gbq.exceptions import GbqException, InvalidDefinitionException
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
    mock_client = mocker.patch("gbq.bigquery.bigquery.Client")
    # Ensure the mock client has a project attribute for QueryJob compatibility
    mock_client.return_value.project = "project"
    # Existing tables are real, unpartitioned Table objects, and update_table returns
    # the updated table as the API does
    mock_client.return_value.get_table.return_value = bigquery.Table(
        "project.dataset.structure"
    )
    mock_client.return_value.update_table.side_effect = lambda table, fields: table
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
    with pytest.raises(InvalidDefinitionException, match="Invalid TimeType 'ADASD'"):
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
                    data_type=bigquery.standard_sql.StandardSqlDataType(
                        type_kind=bigquery.StandardSqlTypeNames.DATE
                    ),
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
    response = bq.delete_dataset("project", "dataset")

    assert response
    bq.bq_client.get_dataset.assert_not_called()
    bq.bq_client.delete_dataset.assert_called_once_with(
        "project.dataset", delete_contents=False
    )


def test_delete_dataset_when_not_exists(bq):
    bq.bq_client.delete_dataset.side_effect = NotFound("")
    response = bq.delete_dataset("project", "dataset")
    assert not response


def test_delete_table_or_view(bq):
    response = bq.delete_table_or_view("project", "dataset", "table")

    assert response
    bq.bq_client.get_table.assert_not_called()
    bq.bq_client.delete_table.assert_called_once_with("project.dataset.table")


def test_delete_table_or_view_when_not_exists(bq):
    bq.bq_client.delete_table.side_effect = NotFound("")
    response = bq.delete_table_or_view("project", "dataset", "table")
    assert not response


def test_execute_raise_exception(bq):
    bq.bq_client.query.side_effect = TypeError

    with pytest.raises(GbqException):
        bq.execute("SELECT * FROM some_table")


def test_execute_raise_no_exception(bq, mocker):
    query = "SELECT * FROM some_table"
    # Use a mock instead of creating a real QueryJob to avoid compatibility issues
    mock_query_job = mocker.Mock(spec=QueryJob)
    bq.bq_client.query.return_value = mock_query_job
    response = bq.execute(query=query)

    assert response == mock_query_job


def test_argument_class_data_type():
    input_json = {"name": "test", "data_type": "string"}
    expected = Argument(**input_json)
    assert expected.name == "test"
    assert expected.data_type == BigQueryDataType.STRING


def test_structure_validate_type():
    input_json = {"view_query": "test"}
    expected = Structure(**input_json)
    assert expected.type == StructureType.view


def test_structure_str_or_list_body():
    input_json = {"body": ["test", "test1", "test2"]}
    expected = Structure(**input_json)
    assert expected.body == "test\ntest1\ntest2"


def test_star_import_exports_public_api():
    namespace: dict = {}
    exec("from gbq import *", namespace)  # noqa: S102
    assert namespace["BigQuery"] is BigQuery
    assert namespace["GbqException"] is GbqException
    assert namespace["InvalidDefinitionException"] is InvalidDefinitionException


def test__get_structure_does_not_mutate_input():
    partition = {"type": "time", "definition": {"type": "day"}}
    raw = {"schema": [], "partition": partition, "body": ["a", "b"]}

    structure = BigQuery._get_structure(raw)

    assert structure.partition.definition.type.value == "DAY"
    assert raw == {"schema": [], "partition": partition, "body": ["a", "b"]}
    assert partition == {"type": "time", "definition": {"type": "day"}}


def test_argument_does_not_mutate_input():
    raw = {"name": "test", "data_type": "string"}
    Argument.model_validate(raw)
    assert raw == {"name": "test", "data_type": "string"}


@pytest.mark.parametrize("json_schema", ["not a schema", None, 42])
def test__get_structure_rejects_unsupported_input(json_schema):
    with pytest.raises(InvalidDefinitionException, match="must be a dict or a list"):
        BigQuery._get_structure(json_schema)


@pytest.mark.parametrize(
    "json_schema, message",
    [
        (
            {"partition": {"type": "hourly", "definition": {"type": "DAY"}}},
            "Invalid PartitionType 'hourly'",
        ),
        (
            {"partition": {"type": "time", "definition": {"type": "weekly"}}},
            "Invalid TimeType 'WEEKLY'",
        ),
        (
            {"body": "SELECT 1", "arguments": [{"name": "a", "data_type": "nope"}]},
            "Invalid BigQueryDataType 'NOPE'",
        ),
        ({"labels": "not-a-dict"}, "Invalid structure definition"),
    ],
)
def test__get_structure_wraps_validation_errors(json_schema, message):
    with pytest.raises(InvalidDefinitionException, match=message):
        BigQuery._get_structure(json_schema)


def test_partition_accepts_enum_type():
    partition = Partition.model_validate(
        {"type": PartitionType.time, "definition": {"type": TimeType.DAY}}
    )
    assert partition.type == PartitionType.time
    assert partition.definition.type == TimeType.DAY


def test_argument_accepts_enum_data_type():
    argument = Argument.model_validate(
        {"name": "test", "data_type": BigQueryDataType.STRING}
    )
    assert argument.data_type == BigQueryDataType.STRING


def test_create_bigquery_with_credentials_object(mocker):
    from_info = mocker.patch(
        "gbq.helpers.service_account.Credentials.from_service_account_info"
    )
    client = mocker.patch("gbq.bigquery.bigquery.Client")
    credentials = mocker.Mock()

    bq = BigQuery(project="project", credentials=credentials)

    from_info.assert_not_called()
    client.assert_called_once_with(credentials=credentials, project="project")
    assert bq.credentials is credentials


def test_create_bigquery_with_application_default_credentials(mocker):
    client = mocker.patch("gbq.bigquery.bigquery.Client")

    bq = BigQuery(project="project")

    client.assert_called_once_with(credentials=None, project="project")
    assert bq.credentials is None


def test_create_bigquery_rejects_svc_account_and_credentials(mocker):
    client = mocker.patch("gbq.bigquery.bigquery.Client")

    with pytest.raises(GbqException, match="not both"):
        BigQuery('{"secret": "secret"}', "project", credentials=mocker.Mock())

    client.assert_not_called()


@pytest.mark.parametrize("delete_contents", [True, False])
def test_delete_dataset_passes_delete_contents(bq, delete_contents):
    assert bq.delete_dataset("project", "dataset", delete_contents=delete_contents)

    bq.bq_client.delete_dataset.assert_called_once_with(
        "project.dataset", delete_contents=delete_contents
    )


def test_delete_dataset_keeps_contents_by_default(bq):
    bq.delete_dataset("project", "dataset")

    _, kwargs = bq.bq_client.delete_dataset.call_args
    assert kwargs["delete_contents"] is False


def test_execute_without_parameters_passes_job_config_through(bq):
    job_config = bigquery.QueryJobConfig(labels={"team": "abc"})

    bq.execute("SELECT 1", job_config=job_config)

    bq.bq_client.query.assert_called_once_with(
        "SELECT 1", job_config=job_config, project=None
    )


def test_execute_with_query_parameters(bq):
    parameter = bigquery.ScalarQueryParameter("name", "STRING", "x' OR '1'='1")

    bq.execute("SELECT * FROM t WHERE name = @name", query_parameters=[parameter])

    _, kwargs = bq.bq_client.query.call_args
    assert kwargs["job_config"].query_parameters == [parameter]


def test_execute_with_query_parameters_does_not_modify_job_config(bq):
    job_config = bigquery.QueryJobConfig(labels={"team": "abc"})
    parameter = bigquery.ScalarQueryParameter("id", "INT64", 1)

    bq.execute("SELECT @id", query_parameters=[parameter], job_config=job_config)

    _, kwargs = bq.bq_client.query.call_args
    sent = kwargs["job_config"]
    assert sent is not job_config
    assert sent.query_parameters == [parameter]
    assert sent.labels == {"team": "abc"}
    assert job_config.query_parameters == []


def test_get_dataset_in_project_passes_project(bq, dataset_list_items):
    bq.bq_client.list_datasets.return_value = dataset_list_items

    bq.get_dataset_in_project("other-project")

    bq.bq_client.list_datasets.assert_called_once_with(project="other-project")


def test_get_table_in_project_keeps_dataset_order(bq, dataset_list_items, tables):
    abc, def_ = tables
    by_dataset = {"project.abc": iter([abc]), "project.def": iter([def_, def_])}
    bq.bq_client.list_datasets.return_value = dataset_list_items
    bq.bq_client.list_tables.side_effect = lambda ref: by_dataset[ref]

    response = bq.get_table_in_project("project", max_workers=2)

    assert response == [abc, def_, def_]
    assert bq.bq_client.list_tables.call_count == 2


def test_get_table_in_project_without_datasets(bq):
    bq.bq_client.list_datasets.return_value = []

    assert bq.get_table_in_project("project") == []
    bq.bq_client.list_tables.assert_not_called()


@pytest.fixture()
def existing_table() -> bigquery.Table:
    table = bigquery.Table("project.dataset.structure")
    table.schema = [
        # As returned by the API: no description, standard SQL type alias
        bigquery.SchemaField.from_api_repr({"name": "id", "type": "INT64"}),
        bigquery.SchemaField.from_api_repr(
            {"name": "name", "type": "STRING", "mode": "REQUIRED"}
        ),
    ]
    return table


@pytest.fixture()
def same_schema() -> list[dict]:
    return [
        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
    ]


def test_update_table_skipped_when_unchanged(bq, existing_table, same_schema):
    bq.bq_client.get_table.return_value = existing_table

    result = bq.create_or_update_structure(
        "project", "dataset", "structure", same_schema
    )

    assert result is existing_table
    bq.bq_client.update_table.assert_not_called()
    bq.bq_client.create_table.assert_not_called()


def test_update_table_sends_only_changed_fields(bq, existing_table, same_schema):
    bq.bq_client.get_table.return_value = existing_table

    bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        {"schema": same_schema, "labels": {"team": "abc"}},
    )

    bq.bq_client.update_table.assert_called_once_with(existing_table, ["labels"])


def test_update_table_sends_changed_schema(bq, existing_table, same_schema):
    bq.bq_client.get_table.return_value = existing_table
    new_schema = [*same_schema, {"name": "email", "type": "STRING"}]

    bq.create_or_update_structure("project", "dataset", "structure", new_schema)

    bq.bq_client.update_table.assert_called_once_with(existing_table, ["schema"])
    assert [field.name for field in existing_table.schema] == ["id", "name", "email"]


def test_update_view_skipped_when_query_unchanged(bq):
    view = bigquery.Table("project.dataset.view")
    view.view_query = "SELECT 1"
    bq.bq_client.get_table.return_value = view

    bq.create_or_update_structure(
        "project", "dataset", "view", {"view_query": "SELECT 1"}
    )

    bq.bq_client.update_table.assert_not_called()


def test_update_view_sends_changed_query(bq):
    view = bigquery.Table("project.dataset.view")
    view.view_query = "SELECT 1"
    bq.bq_client.get_table.return_value = view

    bq.create_or_update_structure(
        "project", "dataset", "view", {"view_query": "SELECT 2"}
    )

    bq.bq_client.update_table.assert_called_once_with(view, ["view_query"])
    assert view.view_query == "SELECT 2"


def test_fetch_rows_uses_query_and_wait(bq, mocker):
    rows = mocker.Mock()
    bq.bq_client.query_and_wait.return_value = rows
    parameter = bigquery.ScalarQueryParameter("id", "INT64", 1)

    result = bq.fetch_rows("SELECT @id", query_parameters=[parameter], max_results=5)

    assert result is rows
    bq.bq_client.query.assert_not_called()
    _, kwargs = bq.bq_client.query_and_wait.call_args
    assert kwargs["max_results"] == 5
    assert kwargs["job_config"].query_parameters == [parameter]


def test_fetch_rows_raise_exception(bq):
    bq.bq_client.query_and_wait.side_effect = TypeError("boom")

    with pytest.raises(GbqException, match="boom"):
        bq.fetch_rows("SELECT 1")


@pytest.mark.parametrize(
    "call",
    [
        lambda bq: bq.get_dataset_in_project("other"),
        lambda bq: bq.get_table_in_project("other"),
        lambda bq: bq.delete_dataset("other", "dataset"),
        lambda bq: bq.get_structure("other", "dataset", "table"),
        lambda bq: bq.delete_table_or_view("other", "dataset", "table"),
        lambda bq: bq.get_routine("other", "dataset", "routine"),
        lambda bq: bq.create_or_update_structure(
            "other", "dataset", "table", [{"name": "id", "type": "INTEGER"}]
        ),
        lambda bq: bq.create_or_update_structure(
            "other", "dataset", "procedure", {"body": "SELECT 1"}
        ),
    ],
)
def test_methods_do_not_change_client_project(bq, call):
    bq.bq_client.list_datasets.return_value = []
    bq.bq_client.project = "home"

    call(bq)

    assert bq.bq_client.project == "home"


def test_execute_passes_project(bq):
    bq.execute("SELECT 1", project="billing-project")

    _, kwargs = bq.bq_client.query.call_args
    assert kwargs["project"] == "billing-project"


def test_fetch_rows_passes_project(bq):
    bq.fetch_rows("SELECT 1", project="billing-project")

    _, kwargs = bq.bq_client.query_and_wait.call_args
    assert kwargs["project"] == "billing-project"


SCHEMA = [
    {"name": "id", "type": "INTEGER"},
    {"name": "ts", "type": "TIMESTAMP"},
]


def make_table(**properties) -> bigquery.Table:
    table = bigquery.Table("project.dataset.structure")
    table.schema = [bigquery.SchemaField.from_api_repr(f) for f in SCHEMA]
    for name, value in properties.items():
        setattr(table, name, value)
    return table


def test_update_clears_labels_description_and_clustering_omitted_from_definition(bq):
    table = make_table(
        labels={"team": "abc", "owner": "me"},
        description="old",
        clustering_fields=["id"],
    )
    bq.bq_client.get_table.return_value = table

    result = bq.create_or_update_structure(
        "project", "dataset", "structure", {"schema": SCHEMA, "labels": {"team": "abc"}}
    )

    bq.bq_client.update_table.assert_called_once_with(
        table, ["labels", "description", "clustering_fields"]
    )
    assert result is table
    assert table.labels == {"team": "abc", "owner": None}
    assert table.description is None
    assert table.clustering_fields is None


def test_update_keeps_matching_labels_description_and_clustering(bq):
    bq.bq_client.get_table.return_value = make_table(
        labels={"team": "abc"}, description="d", clustering_fields=["id"]
    )

    bq.create_or_update_structure(
        "project",
        "dataset",
        "structure",
        {
            "schema": SCHEMA,
            "labels": {"team": "abc"},
            "description": "d",
            "clustering": ["id"],
        },
    )

    bq.bq_client.update_table.assert_not_called()


def time_partition(type_="DAY", field="ts", expiration_ms=None):
    return bigquery.TimePartitioning(
        type_=type_, field=field, expiration_ms=expiration_ms
    )


def range_partition(field="id", start=0, end=100, interval=10):
    partitioning = bigquery.RangePartitioning(field=field)
    partitioning.range_ = PartitionRange(start=start, end=end, interval=interval)
    return partitioning


TIME_DEFINITION = {"type": "time", "definition": {"type": "DAY", "field": "ts"}}
RANGE_DEFINITION = {
    "type": "range",
    "definition": {
        "field": "id",
        "range": {"start": 0, "end": 100, "interval": 10},
    },
}


@pytest.mark.parametrize(
    "properties, partition",
    [
        ({"time_partitioning": time_partition()}, TIME_DEFINITION),
        ({"range_partitioning": range_partition()}, RANGE_DEFINITION),
        ({}, None),
    ],
)
def test_update_accepts_unchanged_partitioning(bq, properties, partition):
    bq.bq_client.get_table.return_value = make_table(**properties)
    definition = {"schema": SCHEMA, "partition": partition}

    bq.create_or_update_structure("project", "dataset", "structure", definition)

    bq.bq_client.update_table.assert_not_called()


def test_update_changes_partition_expiration(bq):
    table = make_table(time_partitioning=time_partition(expiration_ms=1000))
    bq.bq_client.get_table.return_value = table
    partition = {
        "type": "time",
        "definition": {"type": "DAY", "field": "ts", "expirationMs": "86400000"},
    }

    bq.create_or_update_structure(
        "project", "dataset", "structure", {"schema": SCHEMA, "partition": partition}
    )

    bq.bq_client.update_table.assert_called_once_with(table, ["time_partitioning"])
    assert table.time_partitioning.expiration_ms == 86400000
    assert table.time_partitioning.field == "ts"


@pytest.mark.parametrize(
    "properties, partition, message",
    [
        ({}, TIME_DEFINITION, "DAY time partitioning on ts"),
        (
            {"time_partitioning": time_partition(type_="HOUR")},
            TIME_DEFINITION,
            "DAY time partitioning",
        ),
        (
            {"time_partitioning": time_partition(field="other")},
            TIME_DEFINITION,
            "time partitioning on ts",
        ),
        (
            {"time_partitioning": time_partition()},
            {"type": "time", "definition": {"type": "DAY"}},
            "on ingestion time",
        ),
        ({"time_partitioning": time_partition()}, None, "definition is not"),
        ({"range_partitioning": range_partition()}, None, "definition is not"),
        ({"time_partitioning": time_partition()}, RANGE_DEFINITION, "range"),
        (
            {"range_partitioning": range_partition(interval=5)},
            RANGE_DEFINITION,
            "range",
        ),
        (
            {"range_partitioning": range_partition(field="ts")},
            RANGE_DEFINITION,
            "range",
        ),
    ],
)
def test_update_rejects_partitioning_changes(bq, properties, partition, message):
    bq.bq_client.get_table.return_value = make_table(**properties)
    definition = {"schema": SCHEMA, "partition": partition}

    with pytest.raises(InvalidDefinitionException, match=message) as exc_info:
        bq.create_or_update_structure("project", "dataset", "structure", definition)

    assert "Cannot change the partitioning of existing table" in str(exc_info.value)
    bq.bq_client.update_table.assert_not_called()


def test_update_keeps_policy_tags_and_precision(bq):
    field = {
        "name": "amount",
        "type": "NUMERIC",
        "precision": "10",
        "scale": "2",
        "policyTags": {"names": ["projects/p/locations/us/taxonomies/1/policyTags/2"]},
    }
    table = bigquery.Table("project.dataset.structure")
    table.schema = [bigquery.SchemaField.from_api_repr(field)]
    bq.bq_client.get_table.return_value = table

    bq.create_or_update_structure("project", "dataset", "structure", [field])

    # Previously the definition lost these attributes, so the schema always differed
    # and the update stripped the column's policy tags
    bq.bq_client.update_table.assert_not_called()


@pytest.mark.parametrize(
    "text, api_repr",
    [
        ("string", {"typeKind": "STRING"}),
        ("json", {"typeKind": "JSON"}),
        (
            "ARRAY<INT64>",
            {"typeKind": "ARRAY", "arrayElementType": {"typeKind": "INT64"}},
        ),
        (
            "struct<id int64, tags ARRAY<STRING>>",
            {
                "typeKind": "STRUCT",
                "structType": {
                    "fields": [
                        {"name": "id", "type": {"typeKind": "INT64"}},
                        {
                            "name": "tags",
                            "type": {
                                "typeKind": "ARRAY",
                                "arrayElementType": {"typeKind": "STRING"},
                            },
                        },
                    ]
                },
            },
        ),
        (
            "ARRAY<STRUCT<DATE, STRING>>",
            {
                "typeKind": "ARRAY",
                "arrayElementType": {
                    "typeKind": "STRUCT",
                    "structType": {
                        "fields": [
                            # The client sends unnamed STRUCT fields with a null name
                            {"name": None, "type": {"typeKind": "DATE"}},
                            {"name": None, "type": {"typeKind": "STRING"}},
                        ]
                    },
                },
            },
        ),
    ],
)
def test_routine_argument_types_sent_to_api(bq, text, api_repr):
    bq.bq_client.get_routine.side_effect = NotFound("")
    bq.bq_client.create_routine.side_effect = lambda routine: routine

    routine = bq.create_or_update_structure(
        "project",
        "dataset",
        "procedure",
        {"body": "SELECT 1", "arguments": [{"name": "x", "data_type": text}]},
    )

    (argument,) = routine.arguments
    assert argument.to_api_repr() == {"name": "x", "dataType": api_repr}


@pytest.mark.parametrize(
    "data_type, message",
    [
        ("ARRAY", "ARRAY needs an element type"),
        ("STRUCT", "STRUCT needs fields"),
        ("ARRAY<ARRAY<INT64>>", "ARRAY of ARRAY"),
        ("ARRAY<INT64", "expected '>', found the end"),
        ("STRUCT<>", "unexpected '>'"),
        ("STRING(10)", "unexpected '\\('"),
        ("ARRAY<NOPE>", "Invalid BigQueryDataType 'NOPE'"),
    ],
)
def test_routine_argument_type_errors(data_type, message):
    definition = {
        "body": "SELECT 1",
        "arguments": [{"name": "x", "data_type": data_type}],
    }

    with pytest.raises(InvalidDefinitionException, match=message):
        BigQuery._get_structure(definition)


def test_argument_data_type_is_top_level_kind():
    argument = Argument(name="x", data_type="ARRAY<INT64>")

    assert argument.data_type == BigQueryDataType.ARRAY
    assert argument.sql_type.array_element_type.type_kind == BigQueryDataType.INT64


def test_argument_rejects_array_enum_without_element_type():
    with pytest.raises(ValidationError, match="ARRAY needs an element type"):
        Argument(name="x", data_type=BigQueryDataType.ARRAY)


def test_argument_round_trips_through_model_dump():
    argument = Argument(name="x", data_type="STRUCT<id INT64>")

    assert Argument.model_validate(argument.model_dump()) == argument


def test_argument_requires_data_type():
    with pytest.raises(ValidationError, match="data_type"):
        Argument.model_validate({"name": "x"})
