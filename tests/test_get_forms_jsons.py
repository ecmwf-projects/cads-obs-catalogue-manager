import json

from cdsobs.forms_jsons import get_forms_jsons


def test_get_forms_jsons(test_repository, tmp_path):
    dataset = "insitu-comprehensive-upper-air-observation-network"
    s3_client = test_repository.s3_client
    bucket = s3_client.get_bucket_name(dataset)
    geco_files = get_forms_jsons(
        dataset,
        test_repository.catalogue_repository,
        tmp_path,
        config=test_repository.config,
        upload_to_storage=True,
        storage_client=s3_client,
        get_stations_file=True,
    )
    for f in geco_files:
        assert f.stat().st_size > 0
        assert s3_client.object_exists(bucket, f.name)
    variables_json = geco_files[2]
    descriptions = json.load(variables_json.open("r"))["CUON"]
    disabled_fields = test_repository.config.get_dataset(dataset).disabled_fields
    assert not any([d in descriptions for d in disabled_fields])
