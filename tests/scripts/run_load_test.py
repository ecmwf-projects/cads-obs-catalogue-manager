import json
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

formatter = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel("INFO")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False


def run_request(client, collection_id, request):
    try:
        client.retrieve(collection_id, **request)
    except Exception as e:
        logger.error(f"Exception {e} captured for request {collection_id=} {request=}")


def main():
    import cads_api_client

    ifile = Path("load_test_requests.jsonl")
    client = cads_api_client.ApiClient()
    test_N = 500
    futures = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        with ifile.open("r") as ifileobj:
            for i, line in enumerate(ifileobj, start=1):
                collection_id, request_params = json.loads(line)
                logger.info(f"Submitting request for {collection_id}, {request_params}")
                future = executor.submit(
                    run_request, client, collection_id, request_params
                )
                futures.append(future)
                if i == test_N:
                    break


if __name__ == "__main__":
    main()
