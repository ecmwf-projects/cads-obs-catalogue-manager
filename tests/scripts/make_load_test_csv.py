import itertools
import json
from pathlib import Path

import numpy
import pandas


def subset_dict(input_dict: dict, keys_to_sel: list) -> dict:
    return {k: v for k, v in input_dict.items() if k not in keys_to_sel}


def expand_constraints_entry_partially(
    entry: dict, not_expand: list[str]
) -> pandas.DataFrame:
    entry_filtered = subset_dict(entry, not_expand)
    constraints_expanded = pandas.DataFrame(
        itertools.product(*entry_filtered.values()), columns=list(entry_filtered.keys())
    )
    for col in not_expand:
        constraints_expanded.loc[:, col] = pandas.Series(
            [entry[col]] * len(constraints_expanded)
        )
    return constraints_expanded


def expand_constraints_partially(
    constraints_entries: list[dict], not_expand: list[str]
) -> pandas.DataFrame:
    constraints_expanded_list = []
    for entry in constraints_entries:
        constraints_entry_expanded = expand_constraints_entry_partially(
            entry, not_expand
        )
        constraints_expanded_list.append(constraints_entry_expanded)
    constraints_expanded = pandas.concat(constraints_expanded_list)
    to_expand = [c for c in constraints_expanded.columns if c not in not_expand]
    # Aggregate the variables that we do not want to have expanded
    constraints_expanded = (
        constraints_expanded.groupby(to_expand)
        .agg({k: "sum" for k in not_expand})
        .reset_index()
    )
    return constraints_expanded


def main():
    collection_ids = [
        "insitu-observations-woudc-ozone-total-column-and-profiles",
        "insitu-observations-igra-baseline-network",
        "insitu-observations-gruan-reference-network",
        "insitu-observations-gnss",
        "insitu-observations-near-surface-temperature-us-climate-reference-network",
    ]

    for collection_id in collection_ids:
        get_load_test_requests_dataset(collection_id)


def get_load_test_requests_dataset(collection_id):
    constraints_dir = Path("../../../cads-forms-json")
    requests_per_type = 33

    with Path(constraints_dir, collection_id, "constraints.json").open("r") as cfile:
        constraints = json.load(cfile)
    # Generate requests
    load_test_requests = []
    # small requests, one day, europe
    small_requests = expand_constraints_partially(constraints, ["variable"]).iloc[
        0:requests_per_type
    ]
    small_requests["area"] = "60,-20,30,50"
    small_requests["area"] = small_requests["area"].str.split(",")
    load_test_requests.append(small_requests)
    # medium requests, one month, one variable, global
    medium_requests = expand_constraints_partially(constraints, ["day"])
    medium_requests = medium_requests.loc[
        ~medium_requests["variable"].str.contains("uncertainty")
    ]
    medium_requests = medium_requests.iloc[0:requests_per_type]
    load_test_requests.append(medium_requests)
    # large requests, one month, all variables
    large_requests = expand_constraints_partially(
        constraints, ["variable", "day"]
    ).iloc[0:requests_per_type]
    load_test_requests.append(large_requests)
    ofilename = f"load_test_requests_{collection_id}.jsonl"
    print(f"Writing {ofilename}")
    with open(ofilename, "w") as ofile:
        for requests_block in load_test_requests:
            for request_params in requests_block.to_dict(orient="records"):
                # Sometimes (10%) ask for a CSV
                if numpy.random.binomial(1, 0.1, 1)[0]:
                    oformat = "csv"
                else:
                    oformat = "netcdf"
                request_params["format"] = oformat
                line = [collection_id, request_params]
                ofile.write(json.dumps(line) + "\n")


if __name__ == "__main__":
    main()
