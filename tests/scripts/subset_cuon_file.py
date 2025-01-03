# Select a few moths only for the tests
from pathlib import Path

import h5py
import numpy
import xarray

from cdsobs.ingestion.core import TimeBatch
from cdsobs.ingestion.readers.cuon import read_nc_file_slices


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


def open_dataset_group(ifile: Path, table_name: str) -> xarray.Dataset:
    return xarray.open_dataset(
        ifile, group=table_name, engine="h5netcdf", decode_times=False
    )


def main(ifile: Path, nfile):
    time_batch = TimeBatch(1960, 1)
    file_and_slices = read_nc_file_slices(ifile, time_batch)
    sorted_by_variable = [
        "advanced_homogenisation",
        "advanced_uncertainty",
        "era5fb",
        "observations_table",
    ]

    ofile = Path(ifile.parent.parent, ifile.name.replace(".nc", "_small.nc"))
    print(f"Writing subset to {ofile}")
    with h5py.File(ifile) as f:
        groups = list(f)

    with open_dataset_group(ifile, "observations_table") as obs_ds:
        varcodes = sorted(set(obs_ds["observed_variable"].values))
        obs_ds_subset_list = []
        for vc in varcodes:
            obs_ds_var = obs_ds.isel(index=file_and_slices.variable_slices[str(vc)])  # type: ignore
            obs_ds_subset_list.append(obs_ds_var)
        obs_ds_subset = xarray.concat(obs_ds_subset_list, dim="index")
        # Get the report ids of the header
        report_ids = numpy.sort(numpy.unique(obs_ds_subset["report_id"].values))
        obs_ds_subset = concat_chars(obs_ds_subset)
        obs_ds_subset.to_netcdf(ofile, mode="a", group="observations_table")

    with open_dataset_group(ifile, "header_table") as header_ds:
        # Get the first 100 times
        header_ds = concat_chars(header_ds)
        report_id_mask = header_ds["report_id"].load().isin(report_ids)
        header_ds = header_ds.sel(index=report_id_mask)
        header_ds = concat_chars(header_ds)
        print("Dates are:", header_ds["report_timestamp"])
        header_ds.to_netcdf(ofile, mode="a", group="header_table")

    tables_remaining = [
        g for g in groups if g not in ["header_table", "observations_id"]
    ]
    for table_name in tables_remaining:
        if table_name in sorted_by_variable:
            with open_dataset_group(ifile, table_name) as table_ds:
                table_ds_subset_list = []
                for vc in varcodes:
                    table_ds_var = table_ds.isel(
                        index=file_and_slices.variable_slices[str(vc)]  # type: ignore
                    )
                    table_ds_subset_list.append(table_ds_var)
                table_ds_subset = xarray.concat(table_ds_subset_list, dim="index")
        else:
            with open_dataset_group(ifile, table_name) as table_ds:
                if len(table_ds.index) == len(report_id_mask):
                    table_ds_subset = table_ds.sel(index=report_id_mask)
                elif table_name == "recordindices":
                    indices = {}

                    for varcode in varcodes:
                        indices[varcode] = numpy.nonzero(
                            obs_ds_subset["observed_variable"].values == int(varcode)
                        )[0]

                    nindices = max([len(ii) for ii in indices.values()])
                    table_ds_subset = table_ds.isel(index=slice(0, nindices)).copy()

                    for varcode in varcodes:
                        varcode_indices = indices[varcode]
                        if len(varcode_indices) == 0:
                            del table_ds_subset[str(varcode)]
                        else:
                            to_pad = nindices - len(varcode_indices)
                            table_ds_subset[str(varcode)][:] = numpy.pad(
                                varcode_indices, (to_pad, 0), mode="edge"
                            )

                    longest_varcode = [
                        vc for vc in varcodes if len(indices[vc]) == nindices
                    ][0]
                    report_ids = obs_ds_subset.isel(
                        index=indices[longest_varcode]
                    ).report_id
                    recordtimestamps = (
                        header_ds.set_index(index="report_id")
                        .sel(index=report_ids)
                        .report_timestamp.values
                    )
                    table_ds_subset["recordtimestamp"][:] = recordtimestamps
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
    for i, ifile in enumerate(ifiles):
        main(ifile, i)
