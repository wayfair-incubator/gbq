import copy
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor

from google.api_core.exceptions import NotFound
from google.auth.credentials import Credentials
from google.cloud import bigquery
from google.cloud.bigquery import (
    ArrayQueryParameter,
    QueryJob,
    QueryJobConfig,
    RangeQueryParameter,
    ScalarQueryParameter,
    StructQueryParameter,
)
from google.cloud.bigquery.dataset import DatasetListItem
from google.cloud.bigquery.routine import Routine, RoutineArgument
from google.cloud.bigquery.table import (
    PartitionRange,
    RowIterator,
    Table,
    TableListItem,
)
from pydantic import ValidationError

from gbq.dto import (
    Partition,
    PartitionType,
    RangeDefinition,
    SqlType,
    Structure,
    StructureType,
    TimeDefinition,
)
from gbq.exceptions import GbqException, InvalidDefinitionException
from gbq.helpers import (
    get_bq_credentials,
    get_bq_schema_from_json_schema,
    schemas_equal,
)

QueryParameter = (
    ScalarQueryParameter
    | ArrayQueryParameter
    | StructQueryParameter
    | RangeQueryParameter
)


class BigQuery:
    """
    BigQueryUtil represent a BigQuery Util resource.

    Authentication uses, in order of precedence: `credentials`, then `svc_account`, then
    Application Default Credentials (for example Workload Identity, an attached service
    account, `GOOGLE_APPLICATION_CREDENTIALS`, or `gcloud auth application-default login`).
    Prefer `credentials` or Application Default Credentials over long-lived service
    account keys.

    Args:
        svc_account (Optional[str]):
            Stringified JSON service account key.
        project (Optional[str]):
            Project bound to the operation.
        credentials (Optional[google.auth.credentials.Credentials]):
            Credentials to use, e.g. from `google.auth.default()` or impersonation.

    Raises:
        GbqException: If both `svc_account` and `credentials` are given, or if
            `svc_account` is not a valid service account key.
    """

    def __init__(
        self,
        svc_account: str | None = None,
        project: str | None = None,
        *,
        credentials: Credentials | None = None,
    ):
        if svc_account is not None and credentials is not None:
            raise GbqException("Pass either svc_account or credentials, not both")

        if svc_account is not None:
            credentials = get_bq_credentials(svc_account)

        # None means the client resolves Application Default Credentials itself
        self.credentials = credentials
        self.bq_client = bigquery.Client(credentials=credentials, project=project)

    def get_dataset_in_project(self, project: str) -> list[DatasetListItem]:
        """
        Function returns list of DatasetListItem objects of all the datasets in a project.

        Args:
            project (str):
                Project bound to the operation.

        Returns:
            List[DatasetListItem]: A list of object of BigQuery DatasetListItem.
        """
        datasets: list[DatasetListItem] = list(
            self.bq_client.list_datasets(project=project)
        )
        return datasets

    def delete_dataset(
        self, project: str, dataset: str, *, delete_contents: bool = True
    ) -> bool:
        """
        Function deletes a dataset.

        Args:
            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the table.
            delete_contents (bool):
                Also delete every table, view and routine in the dataset. Pass False to
                only delete an empty dataset; BigQuery then rejects deleting a non-empty
                one. Defaults to True, which will change to False in the next major
                release.

        Returns:
            Bool: Whether dataset was deleted or not.
        """
        try:
            self.bq_client.delete_dataset(
                f"{project}.{dataset}", delete_contents=delete_contents
            )
        except NotFound:
            return False

        return True

    def get_table_in_project(
        self, project: str, max_workers: int = 8
    ) -> list[TableListItem]:
        """
        Function returns list of TableListItem objects of all the tables and views in a project.

        Tables are listed for up to `max_workers` datasets concurrently, instead of one
        dataset at a time, so a project with many datasets takes a fraction of the time.

        Args:
            project (str):
                Project bound to the operation.
            max_workers (int):
                Maximum number of datasets to list tables for at once. The default stays
                below the HTTP connection pool size of the underlying client.

        Returns:
            List[TableListItem]: A list of object of BigQuery TableListItem, grouped by
                dataset in the order the datasets are listed.
        """
        datasets: list[DatasetListItem] = self.get_dataset_in_project(project)
        if not datasets:
            return []

        def list_tables(dataset: DatasetListItem) -> list[TableListItem]:
            # list() pages through the results inside the worker thread
            return list(
                self.bq_client.list_tables(f"{dataset.project}.{dataset.dataset_id}")
            )

        with ThreadPoolExecutor(max_workers=min(max_workers, len(datasets))) as pool:
            tables_per_dataset = pool.map(list_tables, datasets)

        return [table for tables in tables_per_dataset for table in tables]

    def get_structure(self, project: str, dataset: str, structure: str) -> Table:
        """
        Function returns a BigQuery Table object.

        Args:
            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the table.
            structure (str):
                ID of the structure, can be a table or a view.

        Returns:
            Table: An object of BigQuery Table.
        """
        full_table_name = f"{project}.{dataset}.{structure}"
        bq_table: Table = self.bq_client.get_table(full_table_name)
        return bq_table

    def delete_table_or_view(self, project: str, dataset: str, structure: str):
        """
        Function deletes table or view.

        Args:
            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the table.
            structure (str):
                ID of the structure.

        Returns:
            Bool: Whether table or view was deleted or not.
        """
        try:
            self.bq_client.delete_table(f"{project}.{dataset}.{structure}")
        except NotFound:
            return False

        return True

    def get_routine(self, project: str, dataset: str, routine_name: str) -> Routine:
        """
        Function returns a BigQuery Table object.

        Args:

            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the table.
            routine_name (str):
                ID of the structure, can be a table or a view.

        Returns:
            Routine: An object of BigQuery Routine.
        """
        routine_id = f"{project}.{dataset}.{routine_name}"
        routine: Routine = self.bq_client.get_routine(routine_id)
        return routine

    def create_or_update_structure(
        self,
        project: str,
        dataset: str,
        structure_id: str,
        json_schema: list[dict] | dict,
    ) -> Table | Routine:
        """
        Function creates/updates provided json schema to the structure.

        Args:
            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the structure.
            structure_id (str):
                ID of the structure.
            json_schema (Union[List[Dict], Dict]):
                Raw JSON schema of the table. If List[Dict] it is assumed that the value is
                    table schema, if Dict than value can include partitioning scheme, clustering,
                    table schema, view query, labels, etc

        Examples:
            List[Dict]:
            [
                {
                    "name": "name1",
                    "type: "INTEGER",
                    "mode: "REQUIRED"
                },
                {
                    "name": "name2",
                    "type: "STRING"
                }...
            ]

            Dict:
            List[Dict]:
            {
                "clustering": ["name"],
                "partition": {
                    "type": "time",
                    "definition": {
                        "type": "DAY"
                    }
                },
                "description": "this is a partitioned table",
                "labels": {"team: "abc"},
                "schema: [
                    {
                        "name": "name1",
                        "type: "INTEGER",
                        "mode: "REQUIRED"
                    },
                    {
                        "name": "name2",
                        "type: "STRING"
                    }...
                ]
            }


        Returns:
            Table: An object of BigQuery Table.
        """
        structure = self._get_structure(json_schema)

        if (
            structure.type == StructureType.table
            or structure.type == StructureType.view
        ):
            return self._handle_table_or_view(dataset, project, structure_id, structure)
        elif structure.type == StructureType.stored_procedure:
            return self._handle_stored_procedure(
                dataset, project, structure_id, structure
            )
        else:
            raise InvalidDefinitionException("Missing required structure definition")

    def _handle_table_or_view(
        self, dataset: str, project: str, structure_id: str, structure: Structure
    ):
        """
        Function creates/updates BigQuery Table per the provided information.

        On update the definition is the source of truth: labels, description and
        clustering that the definition omits are removed from the existing table.

        Args:
            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the table.
            structure_id (str):
                ID of the table or view.
            structure (Structure):
                An object of internal Structure class.

        Returns:
            Table: An object of BigQuery Table.

        Raises:
            InvalidDefinitionException: If the definition's partitioning differs from the
                existing table's in a way BigQuery cannot change in place.
        """
        fields_to_update = []

        try:
            bq_structure = self.get_structure(project, dataset, structure_id)

            if structure.type == StructureType.table:
                schema = get_bq_schema_from_json_schema(structure.table_schema)
                if not schemas_equal(schema, list(bq_structure.schema)):
                    fields_to_update.append("schema")
                    bq_structure.schema = schema
                if self._update_partitioning(bq_structure, structure.partition):
                    fields_to_update.append("time_partitioning")
            elif structure.type == StructureType.view:
                if structure.view_query != bq_structure.view_query:
                    fields_to_update.append("view_query")
                    bq_structure.view_query = structure.view_query

            current_labels = bq_structure.labels or {}
            if structure.labels != current_labels:
                fields_to_update.append("labels")
                # BigQuery only removes a label when it is sent with a None value
                removed = {
                    key: None for key in current_labels if key not in structure.labels
                }
                bq_structure.labels = {**removed, **structure.labels}

            if structure.description != bq_structure.description:
                fields_to_update.append("description")
                bq_structure.description = structure.description

            clustering = structure.clustering or None
            if clustering != (bq_structure.clustering_fields or None):
                fields_to_update.append("clustering_fields")
                bq_structure.clustering_fields = clustering

            # Skip the API call when nothing changed; BigQuery rate-limits table
            # metadata updates, and deploy pipelines re-apply unchanged definitions.
            if fields_to_update:
                return self.bq_client.update_table(bq_structure, fields_to_update)

            return bq_structure
        except NotFound:
            return self._handle_create_structure(
                dataset, project, structure_id, structure
            )

    def _update_partitioning(
        self, bq_structure: Table, partition: Partition | None
    ) -> bool:
        """
        Function checks the definition's partitioning against an existing table.

        BigQuery cannot add, remove or change the partitioning of an existing table, with
        one exception: a time-partitioned table's partition expiration.

        Args:
            bq_structure (Table):
                The existing BigQuery Table. Its partition expiration is updated in place
                when only the expiration differs.
            partition (Optional[Partition]):
                The definition's partitioning.

        Returns:
            Bool: Whether the partition expiration changed and must be sent as an update.

        Raises:
            InvalidDefinitionException: If the partitioning differs in any other way.
        """
        current_time = bq_structure.time_partitioning
        current_range = bq_structure.range_partitioning

        def mismatch(detail: str) -> InvalidDefinitionException:
            return InvalidDefinitionException(
                f"Cannot change the partitioning of existing table "
                f"{bq_structure.full_table_id or bq_structure.table_id}: {detail}. "
                "BigQuery only allows changing the partition expiration in place; "
                "recreate the table to change anything else."
            )

        if partition is None:
            if current_time or current_range:
                raise mismatch("the table is partitioned but the definition is not")
            return False

        if partition.type == PartitionType.time:
            desired_time = self._get_time_partitioned_scheme(partition)
            if (
                current_time is None
                or current_time.type_ != desired_time.type_
                or current_time.field != desired_time.field
            ):
                raise mismatch(
                    f"the definition has {desired_time.type_} time partitioning on "
                    f"{desired_time.field or 'ingestion time'}"
                )
            if current_time.expiration_ms == desired_time.expiration_ms:
                return False
            current_time.expiration_ms = desired_time.expiration_ms
            bq_structure.time_partitioning = current_time
            return True

        desired_range = self._get_range_partitioned_scheme(partition)
        if (
            current_range is None
            or current_range.field != desired_range.field
            or current_range.range_.start != desired_range.range_.start
            or current_range.range_.end != desired_range.range_.end
            or current_range.range_.interval != desired_range.range_.interval
        ):
            raise mismatch(
                f"the definition has range partitioning on {desired_range.field}"
            )
        return False

    @staticmethod
    def _get_structure(json_schema: dict | list[dict]) -> Structure:
        """
        Function returns an object of Structure, curated from the input.

        Args:
            json_schema (Union[List[Dict], Dict]):
                Raw JSON schema of the table. If List[Dict] it is assumed that the value is
                    table schema, if Dict than value can include partitioning scheme, clustering
                    and table schema.

        Returns:
            Structure: An object of Structure.

        Raises:
            InvalidDefinitionException: If json_schema is not a dict or list, or does
                not describe a valid structure.
        """
        if isinstance(json_schema, list):
            json_schema = {"schema": json_schema}
        elif not isinstance(json_schema, dict):
            raise InvalidDefinitionException(
                "Structure definition must be a dict or a list of schema fields, "
                f"got {type(json_schema).__name__}"
            )

        try:
            return Structure.model_validate(json_schema)
        except ValidationError as e:
            raise InvalidDefinitionException(
                f"Invalid structure definition: {e}"
            ) from e

    def _handle_create_structure(
        self, dataset: str, project: str, structure_id: str, structure: Structure
    ) -> Table:
        """
        Function returns an object of BigQuery Table. It is used for creating new table.

        Args:
            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the table.
            structure_id (str):
                ID of the table.
            structure (Structure):
                An object of internal TableSchema class.

        Returns:
            Table: An object of BigQuery Table.
        """
        # Create BQ Table object
        bq_structure = bigquery.Table(f"{project}.{dataset}.{structure_id}")

        if structure.table_schema:
            # Get BQ Schema from JSON provided
            schema = get_bq_schema_from_json_schema(structure.table_schema)
            bq_structure.schema = schema

            # Configure Partition
            if structure.partition:
                bq_structure = self._add_partitioning_scheme(
                    bq_structure=bq_structure, partition_scheme=structure.partition
                )

            # Configure Clustering
            bq_structure.clustering_fields = structure.clustering  # type: ignore

        if structure.view_query:
            bq_structure.view_query = structure.view_query

        if structure.labels:
            bq_structure.labels = structure.labels

        if structure.description:
            bq_structure.description = structure.description

        self.bq_client.create_table(bq_structure)
        return bq_structure

    def _handle_stored_procedure(
        self, dataset: str, project: str, structure_id: str, structure: Structure
    ) -> Routine:
        """
        Function creates/updates BigQuery routine per the provided body and arguments.

        NOTE: Currently we only support PROCEDURE type routines written in SQL. Argument
        types can be any BigQuery type, e.g. "INT64", "ARRAY<STRING>" or
        "STRUCT<id INT64, name STRING>".

        Args:
            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the table.
            structure_id (str):
                ID of the routine.
            structure (Structure):
                An object of internal Structure class.

        Returns:
            Routine: An object of BigQuery Routine.
        """
        routine_id = f"{project}.{dataset}.{structure_id}"

        try:
            routine = self.get_routine(project, dataset, structure_id)
            routine.body = structure.body
            routine.arguments = self._handle_routine_arguments(structure)
            routine.description = structure.description

            routine = self.bq_client.update_routine(
                routine,
                [
                    "body",
                    # Due to a limitation of the API,
                    # all fields are required, not just
                    # those that have been updated.
                    "arguments",
                    "language",
                    "type_",
                    "return_type",
                    "description",
                ],
            )
        except NotFound:
            routine = Routine(
                routine_id,
                type_="PROCEDURE",
                language="SQL",
                body=structure.body,
            )
            routine.description = structure.description
            routine.arguments = self._handle_routine_arguments(structure)

            routine = self.bq_client.create_routine(routine)
        return routine

    @staticmethod
    def _handle_routine_arguments(structure: Structure) -> list[RoutineArgument]:
        """
        Function returns list of objects RoutineArgument type if any specified

        Args:
            structure (Structure):
                An object of internal Structure class.

        Returns:
            List[RoutineArgument]: List of objects of RoutineArgument
        """
        bq_arguments = []
        if structure.arguments:
            for argument in structure.arguments:
                arg = RoutineArgument(
                    name=argument.name,
                    data_type=BigQuery._to_standard_sql_type(argument.sql_type),
                )
                bq_arguments.append(arg)
        return bq_arguments

    @staticmethod
    def _to_standard_sql_type(sql_type: SqlType) -> bigquery.StandardSqlDataType:
        """
        Function converts a parsed SqlType into a BigQuery StandardSqlDataType, including
        ARRAY element types and STRUCT fields, which BigQuery requires.

        Args:
            sql_type (SqlType):
                An object of internal SqlType.

        Returns:
            bigquery.StandardSqlDataType: The equivalent BigQuery type.
        """
        array_element_type = None
        if sql_type.array_element_type is not None:
            array_element_type = BigQuery._to_standard_sql_type(
                sql_type.array_element_type
            )

        struct_type = None
        if sql_type.struct_fields:
            struct_type = bigquery.StandardSqlStructType(
                fields=[
                    bigquery.StandardSqlField(
                        name=field.name,
                        type=BigQuery._to_standard_sql_type(field.type),
                    )
                    for field in sql_type.struct_fields
                ]
            )

        return bigquery.StandardSqlDataType(
            type_kind=bigquery.StandardSqlTypeNames[sql_type.type_kind.value],
            array_element_type=array_element_type,
            struct_type=struct_type,
        )

    def _add_partitioning_scheme(
        self, bq_structure: Table, partition_scheme: Partition
    ) -> Table:
        """
        Function adds partitioning scheme to the table based on the passed configuration.

        Args:
            bq_structure (Table):
                An object of BigQuery Table.
            partition_scheme (Partition):
                An object of internal Partition.

        Returns:
            Table: An object of BigQuery Table.
        """
        if partition_scheme.type.value == PartitionType.time.value:
            time_partitioning = self._get_time_partitioned_scheme(partition_scheme)
            bq_structure.time_partitioning = time_partitioning
        elif partition_scheme.type.value == PartitionType.range.value:
            range_partitioning = self._get_range_partitioned_scheme(partition_scheme)
            bq_structure.range_partitioning = range_partitioning

        return bq_structure

    @staticmethod
    def _get_time_partitioned_scheme(
        partition_scheme: Partition,
    ) -> bigquery.TimePartitioning:
        """
        Function returns a BQ Time Partitioning scheme from Partition object.

        Args:
            partition_scheme (Partition):
                An object of internal Partition.

        Returns:
            bigquery.TimePartitioning: An object of bigquery.TimePartitioning.
        """
        definition: TimeDefinition = partition_scheme.definition  # type: ignore
        time_partitioning = bigquery.TimePartitioning(type_=definition.type.value)
        if definition.field:
            time_partitioning.field = definition.field
        if definition.expirationMs:
            time_partitioning.expiration_ms = definition.expirationMs
        return time_partitioning

    @staticmethod
    def _get_range_partitioned_scheme(
        partition_scheme: Partition,
    ) -> bigquery.RangePartitioning:
        """
        Function returns a BQ Range Partitioning scheme from Partition object.

        Args:
            partition_scheme (Partition):
                An object of internal Partition.

        Returns:
            bigquery.RangePartitioning: An object of bigquery.TimePartitioning.
        """
        definition: RangeDefinition = partition_scheme.definition  # type: ignore
        range_partitioning = bigquery.RangePartitioning(field=definition.field)
        range_partitioning.range_ = PartitionRange(**definition.range.__dict__)
        return range_partitioning

    def execute(
        self,
        query: str,
        query_parameters: Sequence[QueryParameter] | None = None,
        job_config: QueryJobConfig | None = None,
        *,
        project: str | None = None,
    ) -> QueryJob:
        """
        Function return a QueryJob object after executing a SQL statement

        Pass untrusted values as query_parameters and reference them in the query as
        `@name`, rather than formatting them into the query string, to avoid SQL injection.

        Args:
            query (str):
                BigQuery query string
            query_parameters (Optional[Sequence[QueryParameter]]):
                Parameters referenced by the query, e.g.
                `bigquery.ScalarQueryParameter("name", "STRING", value)`.
            job_config (Optional[QueryJobConfig]):
                Extra job settings such as labels, dry_run or maximum_bytes_billed. It is
                not modified; query_parameters replace any parameters it already has.
            project (Optional[str]):
                Project to run (and bill) the query job in. Defaults to the project the
                BigQuery object was created with. Tables in other projects can still be
                queried by their fully qualified names.

        Examples:
            bq.execute(
                "SELECT * FROM `project.dataset.users` WHERE name = @name",
                query_parameters=[
                    bigquery.ScalarQueryParameter("name", "STRING", user_input)
                ],
            )

        Returns:
            QueryJob
                An object of QueryJob.
        """
        job_config = self._build_job_config(query_parameters, job_config)

        try:
            query_job = self.bq_client.query(
                query, job_config=job_config, project=project
            )

            # Wait for query job to finish.
            query_job.result()

            return query_job
        except Exception as e:
            raise GbqException(str(e)) from e

    def fetch_rows(
        self,
        query: str,
        query_parameters: Sequence[QueryParameter] | None = None,
        job_config: QueryJobConfig | None = None,
        max_results: int | None = None,
        *,
        project: str | None = None,
    ) -> RowIterator:
        """
        Function runs a SQL statement and returns its rows.

        Unlike execute(), this uses the BigQuery jobs.query fast path, which runs short
        queries in a single request and returns the first page of rows with the response,
        instead of creating a job and polling it. Use execute() when you need the QueryJob
        (job ID, statistics, destination table); use this when you only need the rows.

        Args:
            query (str):
                BigQuery query string
            query_parameters (Optional[Sequence[QueryParameter]]):
                Parameters referenced by the query as `@name`; see execute().
            job_config (Optional[QueryJobConfig]):
                Extra job settings; see execute(). It is not modified.
            max_results (Optional[int]):
                Maximum number of rows to return.
            project (Optional[str]):
                Project to run (and bill) the query in; see execute().

        Returns:
            RowIterator: The query's rows.
        """
        job_config = self._build_job_config(query_parameters, job_config)

        try:
            return self.bq_client.query_and_wait(
                query, job_config=job_config, max_results=max_results, project=project
            )
        except Exception as e:
            raise GbqException(str(e)) from e

    @staticmethod
    def _build_job_config(
        query_parameters: Sequence[QueryParameter] | None,
        job_config: QueryJobConfig | None,
    ) -> QueryJobConfig | None:
        """
        Function returns the job config to send, with query_parameters applied to a copy
        so the caller's job_config is left unchanged.
        """
        if query_parameters is None:
            return job_config

        job_config = copy.deepcopy(job_config) if job_config else QueryJobConfig()
        job_config.query_parameters = list(query_parameters)
        return job_config
