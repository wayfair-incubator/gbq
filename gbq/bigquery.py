from typing import Dict, List, Union

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, bigquery_v2
from google.cloud.bigquery.dataset import DatasetListItem
from google.cloud.bigquery.routine import Routine, RoutineArgument
from google.cloud.bigquery.table import PartitionRange, Table

from gbq.dto import (
    Partition,
    PartitionType,
    RangeDefinition,
    Structure,
    StructureType,
    TimeDefinition,
)
from gbq.exceptions import InvalidDefinitionException
from gbq.helpers import get_bq_credentials, get_bq_schema_from_json_schema


class BigQuery:
    """
    BigQueryUtil represent a BigQuery Util resource.
    Args:
        svc_account (str):
            Stringified JSON service account value.
        project (Optional[str]):
            Project bound to the operation.
    """

    def __init__(self, svc_account: str, project: str = None):
        self.credentials = get_bq_credentials(svc_account)
        self.bq_client = bigquery.Client(credentials=self.credentials, project=project)

    def get_dataset_in_project(self, project: str) -> List[DatasetListItem]:
        """
        Function returns list of DatasetListItem objects of all the datasets in a project.

        Args:
            project (str):
                Project bound to the operation.

        Returns:
            List[DatasetListItem]: A list of object of BigQuery DatasetListItem.
        """
        self.bq_client.project = project
        datasets: List[DatasetListItem] = list(self.bq_client.list_datasets())
        return datasets

    def delete_dataset(self, project: str, dataset: str):
        """
        Function deletes a dataset.

        Args:
            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the table.

        Returns:
            Bool: Whether dataset was deleted or not.
        """
        self.bq_client.project = project

        try:
            bq_structure = self.bq_client.get_dataset(dataset)
            self.bq_client.delete_dataset(bq_structure, delete_contents=True)
        except NotFound:
            return False

        return True

    def get_table_in_project(self, project: str) -> List[Table]:
        """
        Function returns list of Table objects of all the tables and views in a project.

        Args:
            project (str):
                Project bound to the operation.

        Returns:
            List[Table]: A list of object of BigQuery Table.
        """
        tables: List[Table] = []
        self.bq_client.project = project
        datasets: List[DatasetListItem] = self.get_dataset_in_project(project)
        for dataset in datasets:
            tables_in_dataset = self.bq_client.list_tables(
                f"{dataset.project}.{dataset.dataset_id}"
            )
            [tables.append(table) for table in tables_in_dataset]  # type: ignore
        return tables

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
        self.bq_client.project = project
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
        self.bq_client.project = project

        try:
            bq_structure = self.get_structure(project, dataset, structure)
            self.bq_client.delete_table(bq_structure)
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
        self.bq_client.project = project
        routine_id = f"{project}.{dataset}.{routine_name}"
        routine: Routine = self.bq_client.get_routine(routine_id)
        return routine

    def create_or_update_structure(
        self,
        project: str,
        dataset: str,
        structure_id: str,
        json_schema: Union[List[Dict], Dict],
    ) -> Union[Table, Routine]:
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
        self.bq_client.project = project

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
        fields_to_update = []

        try:
            bq_structure = self.get_structure(project, dataset, structure_id)

            if structure.type == StructureType.table:
                fields_to_update.append("schema")
                schema = get_bq_schema_from_json_schema(structure.table_schema)
                bq_structure.schema = schema
            elif structure.type == StructureType.view:
                fields_to_update.append("view_query")
                bq_structure.view_query = structure.view_query

            if structure.labels and structure.labels != bq_structure.labels:
                fields_to_update.append("labels")
                bq_structure.labels = structure.labels

            if (
                structure.description
                and structure.description != bq_structure.description
            ):
                fields_to_update.append("description")
                bq_structure.description = structure.description

            if (
                structure.clustering
                and structure.clustering != bq_structure.clustering_fields
            ):
                fields_to_update.append("clustering")
                bq_structure.clustering_fields = structure.clustering  # type: ignore

            self.bq_client.update_table(bq_structure, fields_to_update)

            return bq_structure
        except NotFound:
            return self._handle_create_structure(
                dataset, project, structure_id, structure
            )

    @staticmethod
    def _get_structure(json_schema: Union[Dict, List[Dict]]) -> Structure:
        """
        Function returns an object of Structure, curated from the input.

        Args:
            json_schema (Union[List[Dict], Dict]):
                Raw JSON schema of the table. If List[Dict] it is assumed that the value is
                    table schema, if Dict than value can include partitioning scheme, clustering
                    and table schema.

        Returns:
            Structure: An object of Structure.
        """
        if isinstance(json_schema, dict):
            return Structure(**json_schema)
        elif isinstance(json_schema, list):
            return Structure(**{"schema": json_schema})

        return  # type: ignore

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

        NOTE: Currently we only support PROCEDURE type routines.

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
        self.bq_client.project = project
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
    def _handle_routine_arguments(structure: Structure) -> List[RoutineArgument]:
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
                    data_type=bigquery_v2.types.StandardSqlDataType(
                        type_kind=argument.data_type.value
                    ),
                )
                bq_arguments.append(arg)
        return bq_arguments

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

    def create_or_update_view(
        self, project: str, dataset: str, view_name: str, sql_schema: str
    ) -> Table:
        """
        Function creates/updates provided sql schema to the structure.

        Args:
            project (str):
                Project bound to the operation.
            dataset (str):
                ID of dataset containing the table.
            view_name (str):
                ID of the view.
            sql_schema (str):
                An object of internal TableSchema class.

        Returns:
            Table: An object of BigQuery Table.
        """
        self.bq_client.project = project

        try:
            bq_structure = self.get_structure(project, dataset, view_name)
            bq_structure.view_query = sql_schema
            self.bq_client.update_table(bq_structure, ["view_query"])
        except NotFound:
            bq_structure = bigquery.Table(f"{project}.{dataset}.{view_name}")
            bq_structure.view_query = sql_schema
            self.bq_client.create_table(bq_structure)

        return bq_structure
