from cdsobs.forms_jsons import get_forms_jsons


def test_get_forms_jsons(test_repository, tmp_path):
    dataset = "insitu-observations-woudc-ozone-total-column-and-profiles"
    s3_client = test_repository.s3_client
    bucket = s3_client.get_bucket_name(dataset)
    geco_files = get_forms_jsons(
        dataset,
        test_repository.catalogue_repository,
        tmp_path,
        upload_to_storage=True,
        storage_client=s3_client,
        get_stations_file=True,
    )
    for f in geco_files:
        assert f.stat().st_size > 0
        assert s3_client.object_exists(bucket, f.name)
