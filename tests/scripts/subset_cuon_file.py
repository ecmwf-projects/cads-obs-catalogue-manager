# Select a few moths only for the tests
from pathlib import Path

import netCDF4
import numpy
import xarray


def concat_chars(in_ds: xarray.Dataset) -> xarray.Dataset:
    out_ds = in_ds.copy()
    for varname in in_ds:
        print(f"Checking {varname}")
        if out_ds[varname].dtype.char == "S" and len(out_ds[varname].shape) > 1:
            out_ds[varname] = out_ds[varname].astype("O").sum(axis=1)
            # Remove all dummy string variables
        if "string" in str(varname):
            out_ds = out_ds.drop_vars([varname])
    for coordname in in_ds.coords:
        if "string" in str(coordname):
            out_ds = out_ds.drop_vars([coordname])
    return out_ds


def main(ifile: Path):
    nreports = 1000
    index_start = 10000
    obs_slice = slice(index_start, index_start + nreports)
    sorted_by_variable = [
        "advanced_homogenisation",
        "advanced_uncertainty",
        "era5fb",
        "observations_table",
    ]

    ofile = Path(ifile.parent.parent, ifile.name.replace(".nc", "_small.nc"))
    print(f"Writing subset to {ofile}")
    with netCDF4.Dataset(ifile) as inc:
        groups = list(inc.groups)

    with xarray.open_dataset(ifile, group="observations_table") as obs_ds:
        # Get the report ids of the header
        obs_ds = obs_ds.isel(index=obs_slice)
        obs_ds = concat_chars(obs_ds)
        report_ids = obs_ds["report_id"]
        obs_ds.to_netcdf(ofile, mode="a", group="observations_table")

    with xarray.open_dataset(ifile, group="header_table") as header_ds:
        # Get the first 100 times
        report_id_mask = header_ds["report_id"].isin(report_ids)
        header_ds = header_ds.sel(index=report_id_mask)
        header_ds = concat_chars(header_ds)
        print("Dates are:", header_ds["report_timestamp"]),
        header_ds.to_netcdf(ofile, mode="a", group="header_table")

    tables_remaining = [
        g for g in groups if g not in ["header_table", "observations_id"]
    ]
    for table_name in tables_remaining:
        if table_name in sorted_by_variable:
            with xarray.open_dataset(ifile, group=table_name) as table_ds:
                table_ds_subset = table_ds.isel(index=obs_slice)
        else:
            with xarray.open_dataset(ifile, group=table_name) as table_ds:
                if len(table_ds.index) == len(report_id_mask):
                    table_ds_subset = table_ds.sel(index=report_id_mask)
                elif table_name == "recordindices":
                    # Recordtimestamp is missing the last value in the stored array+
                    # We have to load all except the last and define it as nan
                    table_ds["recordtimestamp"] = xarray.DataArray(
                        numpy.append(
                            table_ds["recordtimestamp"].isel(index=slice(0, -1)).values,
                            numpy.nan,
                        ),
                        coords=dict(index=table_ds.index),
                        dims="index",
                    )
                    report_id_mask_recordindices = numpy.append(report_id_mask, True)
                    table_ds_subset = table_ds.sel(index=report_id_mask_recordindices)
                else:
                    table_ds_subset = table_ds
                if table_name == "station_configuration_codes":
                    # This seems to be empty
                    table_ds_subset = table_ds_subset.drop_vars("index")
        # Concatenating characters to avoid
        table_ds_subset = concat_chars(table_ds_subset)
        print(f"Writing data for table {table_name}")
        table_ds_subset.to_netcdf(ofile, mode="a", group=table_name)


if __name__ == "__main__":
    ifiles = [
        Path("../data/cuon_data/old/0-20001-0-53845_CEUAS_merged_v3.nc"),
        Path("../data/cuon_data/old/0-20001-0-53772_CEUAS_merged_v3.nc"),
    ]
    for ifile in ifiles:
        main(ifile)
