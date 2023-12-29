# Define a table for the variable-group-matching - for now just a dict:
# I do not know all those variables, so some might be in the wrong group - please
# have a look at: https://github.com/glamod/common_data_model/tree/master/table_definitions
# to create the table for the matching observations_table - contains all the
# observations - one row for each observation moment
import warnings
from collections import UserDict
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Any, Iterable, Iterator, List

import pandas

""" constants """

STATION_COLUMN = "primary_station_id"
# These are the tables we are using for the moment by default. Other list can
# be optinally specified
DEFAULT_CDM_TABLES_TO_USE = [
    "station_configuration",
    "observations_table",
    "header_table",
]


@dataclass
class ForeignKey:
    """
    Represents a foreign key in a CDM table.

    Parameters
    ----------
    name:
        Name of the field in the local (i.e. child) table.
    external_name:
        Name of the field in the external (i.e. parent) table.
    external_table:
        Name of the external (parent) table.
    local_table:
        Name of the child table.
    """

    name: str
    external_name: str
    external_table: str
    local_table: str


@dataclass
class CDMTable:
    name: str
    table: pandas.DataFrame

    def __post_init__(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            primary_keys = self.table.loc[
                self.table.kind.str.contains("(pk)")
            ].index.tolist()
        self.primary_keys = primary_keys

    @property
    def fields(self) -> List[str]:
        return self.table.index.tolist()

    @property
    def dtypes(self) -> List[str]:
        return [self.get_cdm_dtype(f) for f in self.fields]

    @property
    def foreign_keys(self) -> Iterator[ForeignKey]:
        # There are some empty fields with spaces (strip solves this)
        entries = self.table.loc[self.table.external_table.str.strip().str.len() > 0]
        foreign_keys = (
            ForeignKey(
                str(name),
                columns.loc["external_table"].split(":")[1],
                columns.loc["external_table"].split(":")[0],
                self.name,
            )
            for name, columns in entries.iterrows()
        )
        return foreign_keys

    def get_cdm_dtype(self, field: str) -> str:
        cdm_dtype = (
            str(self.table.loc[field, "kind"])
            .replace(" (pk)", "")
            .replace("*", "")
            .strip()
        )
        return cdm_dtype


def get_dupes(ilist: Iterable) -> set:
    """
    Find duplicates in a iterable.

    moooeeeep solution from
    https://stackoverflow.com/questions/9835762/how-do-i-find-the-duplicates-in-a-list-and-create-another-list-with-them
    """
    seen: set[Any] = set()
    seen_add = seen.add
    # adds all elements it doesn't know yet to seen and all other to seen_twice
    # seen_add(x) evaluates always to False
    return set([x for x in ilist if x in seen or seen_add(x)])


class CDMTables(UserDict[str, CDMTable]):
    """
    Dict-like object that contains the CDM tables.

    It als contains some useful methods and attributes.
    """

    # These lists are useful to normalize and de-normalize the CDM following a logical
    # order
    header_configuration_tables = [
        "profile_configuration",
        "source_configuration",
        "station_configuration",
    ]
    observation_configuration_tables = ["sensor_configuration"]
    configuration_tables = (
        header_configuration_tables + observation_configuration_tables
    )
    optional_tables = [t + "_optional" for t in configuration_tables]
    fields_tables = ["homogenenisation_fields", "qc_fields", "uncertainty_fields"]

    def __init__(self, cdm_tables_dict: dict[str, CDMTable]):
        UserDict.__init__(self)
        self.update(cdm_tables_dict)

    @property
    def all_foreign_keys(self) -> List[ForeignKey]:
        """All the foreign keys in the tables loaded."""
        return list(chain.from_iterable([t.foreign_keys for t in self.values()]))

    @property
    def non_unique_fields(self) -> List[str]:
        """Fields names that are reused in different tables."""
        all_fields = chain.from_iterable([table.fields for table in self.values()])
        dupes = get_dupes(all_fields)
        foreign_keys_left_names = [f.name for f in self.all_foreign_keys]
        ambiguous_dupes = [d for d in dupes if d not in foreign_keys_left_names]
        return ambiguous_dupes

    def get_children(self, table_name: str) -> List[str]:
        """Tables with foreign keys pointing to a given table."""
        children = [
            fk.local_table
            for fk in self.all_foreign_keys
            if fk.external_table == table_name and fk.external_table in self
        ]
        return sorted(set(children))


def read_cdm_table(cdm_tables_location: Path, name: str) -> CDMTable:
    """
    Read a Common Data Model table from a CSV text file.

    The files are locate din the git submodule in cdsobs/cdm.

    Parameters
    ----------
    cdm_tables_location: Path
      Location of the CDM tables.

    name :
        Name of the CDM table to read.

    Returns
    -------
    CDMTable object which contains the name and a pandas.DataFrame with the data.
    """
    table_path = Path(
        cdm_tables_location, f"common_data_model/table_definitions/{name}.csv"
    )
    table_data = pandas.read_csv(
        table_path,
        delimiter="\t",
        quoting=3,
        dtype=str,
        na_filter=False,
        comment="#",
        index_col="element_name",
    ).drop("description", axis=1)
    # In some tables kind is called type
    if "kind" not in table_data:
        table_data = table_data.rename(dict(type="kind"), axis=1)
    if name == "header_table":
        # Remove the self-referencing "duplicates" field
        table_data = table_data.drop("duplicates")
    if name == "uncertainty_table":
        table_data.loc["observation_id", "kind"] = "varchar (pk)"
        table_data.loc["uncertainty_type", "kind"] = "int (pk)"
        table_data.loc["uncertainty_type", "external_table"] = "uncertainty_type:type"
    return CDMTable(name, table_data)


def read_cdm_tables(
    cdm_tables_location: Path,
    tables_to_use: List[str] = DEFAULT_CDM_TABLES_TO_USE,
) -> CDMTables:
    """
    Read the Common Data Model tables.

    The tables are read from the tables_definition folder of the CDM repository, which
    is checked out as a git submodule in cdsobs/cdm folder.

    Parameters
    ----------
    cdm_tables_location: Path
      Location of the CDM tables.
    tables_to_use :
        If set, read only these tables. Else it will read DEFAULT_CDM_TABLES_TO_USE.

    Returns
    -------
    A CDMTables object which is a dict of pandas.Dataframe with some extra methods.
    """
    if tables_to_use is None:
        cdm_tables_to_use = DEFAULT_CDM_TABLES_TO_USE
    else:
        cdm_tables_to_use = tables_to_use
    cdm_tables_dict = {
        table: read_cdm_table(cdm_tables_location, table) for table in cdm_tables_to_use
    }
    cdm_tables = CDMTables(cdm_tables_dict)
    return cdm_tables
