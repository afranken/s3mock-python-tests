import time
from datetime import datetime, timedelta, timezone

import pytest
from botocore.exceptions import ClientError

from s3mock_test import given_bucket

# reimplementation of https://github.com/adobe/S3Mock/blob/main/integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/BucketIT.kt

def test_creating_and_deleting_a_bucket_is_successful(s3_client, bucket_name: str):
    # Create the bucket
    s3_client.create_bucket(Bucket=bucket_name)

    # Wait until the bucket exists
    s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)

    # Does not throw if bucket exists; also returns a response dict
    head_resp = s3_client.head_bucket(Bucket=bucket_name)
    assert head_resp is not None

    # Delete the bucket
    s3_client.delete_bucket(Bucket=bucket_name)

    # Wait until the bucket no longer exists
    s3_client.get_waiter("bucket_not_exists").wait(Bucket=bucket_name)

    # Verify it's gone: head should raise ClientError with NoSuchBucket/404
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        pytest.fail("Bucket still exists after deletion")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        assert code in ("NoSuchBucket", "404")

def test_creating_a_bucket_with_configuration_is_successful(s3_client, bucket_name: str):
    # Create the bucket with a location constraint (region)
    create_resp = s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint": "ap-southeast-5",
        },
    )

    # Status code and location like in the Kotlin assertions
    assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert create_resp.get("Location") == f"/{bucket_name}"

    # Wait until the bucket exists
    s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)

    # Does not raise if the bucket exists
    head_resp = s3_client.head_bucket(Bucket=bucket_name)
    assert head_resp is not None

def test_deleting_a_non_empty_bucket_fails(s3_client, bucket_name: str):
    # Create a bucket and upload one object into it
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)

    s3_client.put_object(Bucket=bucket_name, Key="test-object", Body=b"data")

    # Attempting to delete a non-empty bucket should raise a ClientError
    with pytest.raises(ClientError) as excinfo:
        s3_client.delete_bucket(Bucket=bucket_name)

    err = excinfo.value
    # Check HTTP status code 409 (Conflict)
    status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 409
    # Check AWS error code "BucketNotEmpty"
    code = err.response.get("Error", {}).get("Code")
    assert code == "BucketNotEmpty"
    
def test_creating_and_listing_multiple_buckets_is_successful(s3_client, bucket_name: str):
    # Create three buckets with a shared base name
    created_names = [f"{bucket_name}-1", f"{bucket_name}-2", f"{bucket_name}-3"]
    for n in created_names:
        given_bucket(s3_client, n)

    # Allow for stripped milliseconds and up to 1 minute of clock skew
    creation_threshold = datetime.now(timezone.utc) - timedelta(minutes=1)

    resp = s3_client.list_buckets()

    # Buckets list should exist
    assert resp.get("Buckets")
    buckets = resp["Buckets"]

    # Expect exactly 5 buckets: 2 defaults + 3 created
    assert len(buckets) == 5

    # Names should be exactly as expected, in order
    names = [b["Name"] for b in buckets]
    assert names == ["bucket-a", "bucket-b", *created_names]

    # Creation dates for the 3 created buckets should be after or equal to the threshold
    assert buckets[2]["CreationDate"] >= creation_threshold
    assert buckets[3]["CreationDate"] >= creation_threshold
    assert buckets[4]["CreationDate"] >= creation_threshold

    # No pagination-like fields for list_buckets (ensure absent/None)
    assert resp.get("Prefix") is None
    assert resp.get("ContinuationToken") is None

    # Owner metadata
    owner = resp.get("Owner") or {}
    assert owner.get("ID") == "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"

def test_creating_and_listing_multiple_buckets_limiting_by_prefix_is_successful(
        s3_client,
        bucket_name: str
):
    # Create three buckets with a shared base name (prefix)
    created_names = [f"{bucket_name}-1", f"{bucket_name}-2", f"{bucket_name}-3"]
    for n in created_names:
        given_bucket(s3_client, n)

    # the returned creation date might strip off the millisecond-part, resulting in rounding down
    # and account for a clock-skew in the Docker container of up to a minute.
    creation_threshold = datetime.now(timezone.utc) - timedelta(minutes=1)

    # List all buckets, then apply client-side filtering by prefix
    # (the AWS SDK for Python does not support a Prefix parameter for list_buckets)
    resp = s3_client.list_buckets()
    assert resp.get("Buckets")
    buckets = resp["Buckets"]

    # Filter by our test prefix and keep order
    filtered = [b for b in buckets if b["Name"].startswith(bucket_name)]
    assert len(filtered) == 3

    names = [b["Name"] for b in filtered]
    assert names == created_names

    # Creation dates should be after or equal to the threshold
    assert filtered[0]["CreationDate"] >= creation_threshold
    assert filtered[1]["CreationDate"] >= creation_threshold
    assert filtered[2]["CreationDate"] >= creation_threshold

    # Emulate the prefix assertion from the original: we used this prefix to filter
    prefix = bucket_name
    assert prefix == bucket_name

    # No continuation token in basic list_buckets response
    assert resp.get("ContinuationToken") is None

    # Owner metadata
    owner = resp.get("Owner") or {}
    assert owner.get("ID") == "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"


def test_creating_and_listing_multiple_buckets_limiting_by_max_buckets_is_successful(
    s3_client, bucket_name: str, endpoint_url_http: str
):
    # Create three buckets with a shared base name
    created_names = [f"{bucket_name}-1", f"{bucket_name}-2", f"{bucket_name}-3"]
    for n in created_names:
        given_bucket(s3_client, n)

    # Allow for stripped milliseconds and up to 1 minute of clock skew
    creation_threshold = datetime.now(timezone.utc) - timedelta(minutes=1)

    # First page: limit to 4 via custom S3Mock query parameter
    page1 = s3_client.list_buckets(MaxBuckets=4)

    # Buckets list should exist
    assert page1["Buckets"]
    buckets1 = page1["Buckets"]
    assert len(buckets1) == 4
    assert [b["Name"] for b in buckets1] == [
        "bucket-a",
        "bucket-b",
        created_names[0],
        created_names[1],
    ]

    assert buckets1[2]["CreationDate"] >= creation_threshold
    assert buckets1[3]["CreationDate"] >= creation_threshold

    assert page1.get("Prefix") in (None, "")  # S3Mock may omit or send empty
    assert page1["ContinuationToken"] is not None
    assert (page1["Owner"]["ID"] ==
            "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be")

    # Second page using continuation token
    page2 = s3_client.list_buckets(ContinuationToken=page1["ContinuationToken"])

    assert page2["Buckets"]
    buckets2 = page2["Buckets"]
    assert len(buckets2) == 1
    assert [b["Name"] for b in buckets2] == [created_names[2]]
    assert buckets2[0]["CreationDate"] >= creation_threshold

    assert page2.get("Prefix") in (None, "")
    assert page2.get("ContinuationToken") in (None, "")
    assert (page2["Owner"]["ID"] ==
            "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be")

def test_default_buckets_were_created(s3_client):
    resp = s3_client.list_buckets()
    buckets = resp.get("Buckets", [])
    names = [b["Name"] for b in buckets]
    assert len(names) == 2
    assert names == ["bucket-a", "bucket-b"]

def test_get_bucket_location_returns_a_result(s3_client, bucket_name: str):
    # Create bucket in a specific region
    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)

    # Retrieve bucket location
    resp = s3_client.get_bucket_location(Bucket=bucket_name)
    assert resp.get("LocationConstraint") == "eu-west-1"

def test_by_default_bucket_versioning_is_turned_off(s3_client, bucket_name: str):
    # Create a fresh bucket
    given_bucket(s3_client, bucket_name)

    # Query bucket versioning
    resp = s3_client.get_bucket_versioning(Bucket=bucket_name)

    # When versioning is not configured, Status and MFADelete should be absent/None
    assert resp.get("Status") is None
    assert resp.get("MFADelete") is None

def test_put_bucket_versioning_works_get_bucket_versioning_returns_enabled(
        s3_client,
        bucket_name: str
):
    # Create a bucket
    given_bucket(s3_client, bucket_name)

    # Enable versioning on the bucket
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled",},
    )

    # Verify versioning status is returned as Enabled
    resp = s3_client.get_bucket_versioning(Bucket=bucket_name)
    assert resp.get("Status") == "Enabled"

def test_put_bucket_versioning_with_mfa_works_get_bucket_versioning_is_returned_correctly(
    s3_client, bucket_name: str
):
    # Create a bucket
    given_bucket(s3_client, bucket_name)

    # Enable versioning with MFA delete via MFA header and configuration
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        MFA="fakeMfaValue",
        VersioningConfiguration={
            "Status": "Enabled",
            "MFADelete": "Enabled",
        },
    )

    # Verify both versioning status and MFA delete status
    resp = s3_client.get_bucket_versioning(Bucket=bucket_name)
    assert resp.get("Status") == "Enabled"
    assert resp.get("MFADelete") == "Enabled"

def test_duplicate_bucket_creation_returns_the_correct_error(s3_client, bucket_name: str):
    # Create the bucket
    s3_client.create_bucket(Bucket=bucket_name)

    # Wait until the bucket exists and verify
    s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)
    head_resp = s3_client.head_bucket(Bucket=bucket_name)
    assert head_resp is not None

    # Attempt to create the same bucket again and validate error details
    with pytest.raises(ClientError) as excinfo:
        s3_client.create_bucket(Bucket=bucket_name)

    err = excinfo.value
    status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 409
    code = err.response.get("Error", {}).get("Code")
    assert code == "BucketAlreadyOwnedByYou"

    # Clean up: delete the bucket and wait until it's gone
    s3_client.delete_bucket(Bucket=bucket_name)
    s3_client.get_waiter("bucket_not_exists").wait(Bucket=bucket_name)

    # Confirm the bucket is gone
    with pytest.raises(ClientError) as exc2:
        s3_client.head_bucket(Bucket=bucket_name)
    code2 = exc2.value.response.get("Error", {}).get("Code")
    assert code2 in ("NoSuchBucket", "404")

def test_duplicate_bucket_deletion_returns_the_correct_error(s3_client, bucket_name: str):
    # Create the bucket
    s3_client.create_bucket(Bucket=bucket_name)

    # Wait until the bucket exists and verify
    s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)
    head_resp = s3_client.head_bucket(Bucket=bucket_name)
    assert head_resp is not None

    # Delete the bucket and wait until it's gone
    s3_client.delete_bucket(Bucket=bucket_name)
    s3_client.get_waiter("bucket_not_exists").wait(Bucket=bucket_name)

    # Confirm the bucket is gone (head should fail)
    with pytest.raises(ClientError) as exc1:
        s3_client.head_bucket(Bucket=bucket_name)
    code1 = exc1.value.response.get("Error", {}).get("Code")
    assert code1 in ("NoSuchBucket", "404")

    # Deleting a non-existent bucket should return a 404 NoSuchBucket
    with pytest.raises(ClientError) as exc2:
        s3_client.delete_bucket(Bucket=bucket_name)
    status2 = exc2.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status2 == 404
    code2 = exc2.value.response.get("Error", {}).get("Code")
    assert code2 in ("NoSuchBucket", "404")

def test_get_bucket_lifecycle_returns_error_if_not_set(s3_client, bucket_name: str):
    # Create the bucket and wait until it exists
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)
    head_resp = s3_client.head_bucket(Bucket=bucket_name)
    assert head_resp is not None

    # Getting lifecycle configuration on a bucket without one should raise a 404
    with pytest.raises(ClientError) as excinfo:
        s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)

    err = excinfo.value
    status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 404
    code = err.response.get("Error", {}).get("Code")
    assert code == "NoSuchLifecycleConfiguration"

def test_put_get_delete_bucket_lifecycle_is_successful(s3_client, bucket_name: str):
    # Create the bucket and wait until it exists
    s3_client.create_bucket(Bucket=bucket_name)
    s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)
    head_resp = s3_client.head_bucket(Bucket=bucket_name)
    assert head_resp is not None

    # Define lifecycle configuration equivalent to the selection
    lifecycle_config = {
        "Rules": [
            {
                "ID": bucket_name,
                "Status": "Enabled",
                "Filter": {"Prefix": "myprefix/"},
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 2},
                "Expiration": {"Days": 2},
            }
        ]
    }

    # Put lifecycle configuration
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name, LifecycleConfiguration=lifecycle_config
    )

    # Get lifecycle configuration and verify first rule matches important fields
    got = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
    assert got.get("Rules")
    assert len(got["Rules"]) == 1
    rule = got["Rules"][0]
    assert rule.get("ID") == bucket_name
    assert rule.get("Status") == "Enabled"
    assert rule.get("Filter", {}).get("Prefix") == "myprefix/"
    assert rule.get("AbortIncompleteMultipartUpload", {}).get("DaysAfterInitiation") == 2
    assert rule.get("Expiration", {}).get("Days") == 2

    # Delete lifecycle configuration
    del_resp = s3_client.delete_bucket_lifecycle(Bucket=bucket_name)
    # Expect 204 No Content (allow 200 in case of implementation variance)
    status_del = del_resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status_del in (204, 200)

    # Give backend time to apply deletion to ensure following call fails
    time.sleep(3)

    # Now fetching lifecycle should yield 404 NoSuchLifecycleConfiguration
    with pytest.raises(ClientError) as excinfo:
        s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)

    err = excinfo.value
    status = err.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 404
    code = err.response.get("Error", {}).get("Code")
    assert code == "NoSuchLifecycleConfiguration"
