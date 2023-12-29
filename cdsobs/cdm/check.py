from dataclasses import dataclass
from typing import Iterator, List

import pandas

from cdsobs.cdm.mappings import cdm_dtypes2numpy
from cdsobs.cdm.tables import CDMTable, CDMTables, ForeignKey
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


class CdmValidationError(RuntimeError):
    pass


@dataclass
class CdmTableFieldsMapping:
    """Maps the fields from input data to a CDM table.

    Parameters
    ----------
    fields_found:
      Fields of the CDM table found as data columns in the input data.
    foreign_fields:
      Foreign keys where fields of the table where found in the input with their names
      from a child table (instead of the names defined in the table, that can be different).
    fields_with_suffix:
      Fields that have a "|table_name" suffix for disambiguation.
    table:
      CDM table for this mapping.
    """

    fields_found: List[str]
    foreign_fields: List[ForeignKey]
    fields_with_suffix: List[str]
    table: CDMTable

    @property
    def all_fields_available(self) -> List[str]:
        """
        All fields available in the input data for this CDM table.

        Including those with names mapped from foreign keys.
        """
        return self.fields_found + [ff.external_name for ff in self.foreign_fields]

    def get_field_name_in_data(self, field_in_table: str) -> str:
        """Return the name of a field in the input data."""
        if field_in_table in self.fields_found:
            field_name = field_in_table
        else:
            field_name = [
                ff.name
                for ff in self.foreign_fields
                if ff.external_name == field_in_table
            ][0]
        if field_name in self.fields_with_suffix:
            field_name += "|" + self.table.name
        return field_name


def check_for_ambiguous_fields(
    cdm_tables: CDMTables, homogenised_data: pandas.DataFrame
):
    """Check for column names that can be mapped to more than one CDM table."""
    ambigous_fields = homogenised_data.columns.intersection(
        cdm_tables.non_unique_fields
    )
    if len(ambigous_fields) > 0:
        logger.warning(
            f"The following fields can be mapped to more than one table: "
            f"{ambigous_fields}. Please specify the table appending '|table_name to the"
            f" field name."
        )


def check_table_cdm_compliance(
    cdm_tables: CDMTables,
    homogenised_data: pandas.DataFrame,
    table_def: CDMTable,
) -> CdmTableFieldsMapping:
    """Run the CDM checks for a given table.

    It returns an object that maps the
    fields of the input data table to the fields of the CDM table.
    """
    table_name = table_def.name
    table_field_mapping = get_cdm_table_mapping(cdm_tables, homogenised_data, table_def)
    logger.info(
        f"Found the following fields for {table_name=}: "
        f"{table_field_mapping.fields_found + table_field_mapping.fields_with_suffix}"
    )
    foreign_fields = table_field_mapping.foreign_fields
    if len(foreign_fields) > 0:
        logger.info(
            "Also, the following fields can be mapped from their names in "
            f"children tables: {foreign_fields=}"
        )
    # Check the primary keys for this table are available and NaN free
    _check_primary_keys(table_field_mapping, homogenised_data)
    # Check data types
    _check_data_types(homogenised_data, table_field_mapping)
    return table_field_mapping


def _check_data_types(
    homogenised_data: pandas.DataFrame,
    table_field_mapping: CdmTableFieldsMapping,
):
    """Check data types against those defined in the CDM tables.

    As the CDM dtypes are sql-like, they are been mapped to numpy dtypes for
    this check."
    """
    logger.info(
        "Checking data types against those defined in the CDM tables. As the "
        "CDM dtypes are sql-like, they have been mapped to numpy dtypes for "
        "this check."
    )
    table_def = table_field_mapping.table
    table_name = table_field_mapping.table.name
    for field in table_field_mapping.fields_found:
        cdm_dtype = (
            str(table_def.table.loc[field, "kind"])
            .replace(" (pk)", "")
            .replace("*", "")
            .strip()
        )
        cdm_numpy_dtype = cdm_dtypes2numpy[cdm_dtype]
        field_in_input_data = (
            field
            if field not in table_field_mapping.fields_with_suffix
            else field + f"|{table_name}"
        )
        input_data_dtype = str(homogenised_data[field_in_input_data].dtype)
        # Check for equalness or if cdm_numpy_dtype it is a list check
        # input_data_dtype is in the list.
        dtype_check_passed = (input_data_dtype == cdm_numpy_dtype) or (
            isinstance(cdm_numpy_dtype, list) and input_data_dtype in cdm_numpy_dtype
        )
        if not dtype_check_passed:
            logger.warning(
                f"For {field=} {input_data_dtype=} does not match with {cdm_numpy_dtype=}"
            )
        else:
            # Warn if timestamp without timeszones is used
            if cdm_dtype == "timestamp with timezone" and not hasattr(
                input_data_dtype, "tz"
            ):
                logger.warning(
                    f"{field=} does not have timezone information, UTC is assumed."
                )


def get_cdm_table_mapping(
    cdm_tables: CDMTables, homogenised_data: pandas.DataFrame, table_def: CDMTable
) -> CdmTableFieldsMapping:
    """Build a CdmTableFieldsMapping object.

    Build an object that maps the fields in the input data frame to the fields
    of a given CDM table.
    """
    table_name = table_def.name
    fields_in_input_data = []
    fields_with_suffix = []
    # Save the input fields, with the |table_name suffix removed for those of the current
    # table.
    for field in homogenised_data.columns:
        if "|" in field:
            field_table = field.split("|")[1]
            if field_table == table_name:
                field_nosuffix = field.split("|")[0]
                fields_in_input_data.append(field_nosuffix)
                fields_with_suffix.append(field_nosuffix)
        else:
            fields_in_input_data.append(field)
    # Check which fields are available in the input data
    fields_found = list(set(fields_in_input_data).intersection(table_def.fields))
    foreign_fields = list(_get_foreign_fields(cdm_tables, homogenised_data, table_name))
    table_field_mapping = CdmTableFieldsMapping(
        fields_found, foreign_fields, fields_with_suffix, table_def
    )
    return table_field_mapping


def _check_primary_keys(
    fields_mapping: CdmTableFieldsMapping, homogenised_data: pandas.DataFrame
):
    """Check that primary keys exist and have not NaN values."""
    table_def = fields_mapping.table
    table_name = table_def.name
    for primary_key in table_def.primary_keys:
        if primary_key not in fields_mapping.all_fields_available:
            logger.warning(f"{primary_key=} not available in table {table_name}.")
        else:
            # Check for nans
            field_name = fields_mapping.get_field_name_in_data(primary_key)
            if homogenised_data[field_name].isnull().any():
                raise CdmValidationError(
                    f"NaN found in {primary_key=}, this is not allowed."
                )


def _get_foreign_fields(
    cdm_tables: CDMTables, homogenised_data: pandas.DataFrame, table_name: str
) -> Iterator[ForeignKey]:
    """For a table, get the fields that can be mapped from their children tables names."""
    children_table_names = cdm_tables.get_children(table_name)
    for children in children_table_names:
        for fk in cdm_tables[children].foreign_keys:
            if (
                fk.external_table == table_name
                and fk.external_name != fk.name
                and fk.name in homogenised_data
            ):
                yield fk
