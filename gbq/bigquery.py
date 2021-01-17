from typing import Dict, List, Union

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery.dataset import DatasetListItem
from google.cloud.bigquery.table import PartitionRange, Table

from gbq.dto import (
    Partition,
    PartitionType,
    RangeDefinition,
    Structure,
    StructureType,
    TimeDefinition,
)
from gbq.helpers import get_bq_credentials, get_bq_schema_from_json_schema


class BigQuery:
    """
    BigQuery represent a BigQuery utility resource.

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

    def upsert_structure(
        self,
        project: str,
        dataset: str,
        structure_id: str,
        json_schema: Union[List[Dict], Dict],
    ) -> Table:
        """
        Function upserts provided json schema to the structure.

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
        fields_to_update = []
        self.bq_client.project = project

        structure = self._get_structure(json_schema)

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
