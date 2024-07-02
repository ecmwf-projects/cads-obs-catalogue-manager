import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import cads_api_client

logging.getLogger("root").setLevel("DEBUG")


def run_request(client, collection_id, request):
    result = client.retrieve(collection_id, **request)
    return result


def main():
    url = "https://cds-dev-bopen.copernicus-climate.eu/api/"
    ifile = Path(
        "load_test_requests_insitu-observations-woudc-ozone-total-column-and-profiles.jsonl"
    )
    client = cads_api_client.ApiClient(url=url)
    test_N = 1
    futures = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        with ifile.open("r") as ifileobj:
            for i, line in enumerate(ifileobj, start=1):
                collection_id, request_params = json.loads(line)
                print(f"Submitting request for {collection_id}, {request_params}")
                future = executor.submit(
                    run_request, client, collection_id, request_params
                )
                futures.append(future)
                if i == test_N:
                    break

            for f in futures:
                print(f.result())


if __name__ == "__main__":
    main()
