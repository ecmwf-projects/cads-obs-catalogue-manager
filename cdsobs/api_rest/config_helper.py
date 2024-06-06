from cdsobs.service_definition.api import get_service_definition


def datasets_installed() -> list[str]:
    return []


def sources_installed() -> dict[str, list[str]]:
    sources = dict()
    for dataset in datasets_installed():
        sources[dataset] = get_dataset_sources(dataset)
    return sources


def get_dataset_sources(dataset: str) -> list[str]:
    service_def = get_service_definition(dataset)
    try:
        return list(service_def.sources.keys())
    except (KeyError, FileNotFoundError):
        raise RuntimeError(f"Invalid service definition for {dataset=}")
