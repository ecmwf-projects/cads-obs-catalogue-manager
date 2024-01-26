def test_s3_client_interface(test_s3_client):
    dataset_name = "insitu-observations-woudc-ozone-total-column-and-profiles"
    actual_bucket_name = test_s3_client.get_bucket_name(dataset_name)
    expected_bucket_name = (
        "cds2-obs-dev-insitu-observations-woudc-ozone-total-column-and-p"
    )
    assert actual_bucket_name == expected_bucket_name
    # Test for removing the last hyphen
    dataset_name = "iinsitu-observations-woudc-ozone-total-column-and-profiles"
    actual_bucket_name = test_s3_client.get_bucket_name(dataset_name)
    expected_bucket_name = (
        "cds2-obs-dev-iinsitu-observations-woudc-ozone-total-column-and"
    )
    assert actual_bucket_name == expected_bucket_name
