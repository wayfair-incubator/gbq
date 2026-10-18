from importlib.metadata import PackageNotFoundError, version

from gbq.bigquery import BigQuery
from gbq.exceptions import GbqException, InvalidDefinitionException

# pyproject.toml is the single source of the version
try:
    __version__ = version("gbq")
except PackageNotFoundError:  # imported from source without being installed
    __version__ = "0.0.0+unknown"

__author__ = "Jash Parekh <jparekh1@wayfair.com>"
__all__ = ["BigQuery", "GbqException", "InvalidDefinitionException"]
