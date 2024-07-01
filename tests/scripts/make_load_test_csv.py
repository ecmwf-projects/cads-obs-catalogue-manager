import itertools
import json
from pathlib import Path

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
    return constraints_expanded


def main():
    collection_ids = [
        "insitu-observations-igra-baseline-network",
        "insitu-observations-gruan-reference-network",
        "insitu-observations-gnss",
        "insitu-observations-near-surface-temperature-us-climate-reference-network",
    ]

    for collection_id in collection_ids:
        get_load_test_requests_dataset(collection_id)


def get_load_test_requests_dataset(collection_id):
    constraints_dir = Path("/home/garciam/git/copds/cads-forms-json")
    requests_per_type = 42

    with Path(constraints_dir, collection_id, "constraints.json").open("r") as cfile:
        constraints = json.load(cfile)
    # Generate requests
    load_test_requests = []
    # small requests, one day
    small_requests = expand_constraints_partially(constraints, ["day"]).iloc[
        0:requests_per_type
    ]
    load_test_requests.append(small_requests)
    # medium requests, one month
    medium_requests = expand_constraints_partially(
        constraints, ["variable", "day"]
    ).iloc[0:requests_per_type]
    load_test_requests.append(medium_requests)
    # large requests, one year
    large_requests = expand_constraints_partially(
        constraints, ["variable", "month", "day"]
    ).iloc[0:requests_per_type]
    load_test_requests.append(large_requests)
    ofilename = f"load_test_requests_{collection_id}.jsonl"
    print(f"Writing {ofilename}")
    with open(ofilename, "w") as ofile:
        for requests_block in load_test_requests:
            for request_params in requests_block.to_dict(orient="records"):
                line = [collection_id, request_params]
                ofile.write(json.dumps(line) + "\n")


if __name__ == "__main__":
    main()
