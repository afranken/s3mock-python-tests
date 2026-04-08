from s3mock_test import UPLOAD_FILE_NAME, given_bucket, given_object

# reimplementation of https://github.com/adobe/S3Mock/blob/main/integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/AclIT.kt

def test_put_canned_acl_returns_ok_get_acl_returns_the_acl(s3_client, bucket_name: str):
    source_key = UPLOAD_FILE_NAME

    # create bucket that sets ownership to non-default to allow setting ACLs.
    s3_client.create_bucket(
        Bucket=bucket_name,
        ObjectOwnership="ObjectWriter"
    )

    # put an object
    given_object(s3_client, bucket_name, source_key)

    # set canned ACL to private
    put_resp = s3_client.put_object_acl(Bucket=bucket_name, Key=source_key, ACL="private")
    assert 200 <= put_resp["ResponseMetadata"]["HTTPStatusCode"] < 300

    # get ACL and validate
    acl_resp = s3_client.get_object_acl(Bucket=bucket_name, Key=source_key)
    assert 200 <= acl_resp["ResponseMetadata"]["HTTPStatusCode"] < 300

    owner = acl_resp.get("Owner", {})
    assert owner.get("ID"), "Owner ID should not be blank"

    grants = acl_resp.get("Grants", [])
    assert len(grants) == 1
    assert grants[0]["Permission"] == "FULL_CONTROL"

def test_get_acl_returns_canned_private_acl(s3_client, bucket_name: str):
    source_key = UPLOAD_FILE_NAME

    # Create bucket and put an object. Default object ACL should be 'private'.
    given_bucket(s3_client, bucket_name)
    given_object(s3_client, bucket_name, source_key)

    # Get ACL
    acl_resp = s3_client.get_object_acl(Bucket=bucket_name, Key=source_key)
    assert 200 <= acl_resp["ResponseMetadata"]["HTTPStatusCode"] < 300

    # Owner checks (matches the owner reported by the ACL)
    owner = acl_resp.get("Owner", {})
    assert owner.get("ID"), "Owner ID should not be blank"

    # Grants: canned 'private' should result in a single FULL_CONTROL grant to the owner
    grants = acl_resp.get("Grants", [])
    assert len(grants) == 1

    grant = grants[0]
    assert grant.get("Permission") == "FULL_CONTROL"

    grantee = grant.get("Grantee", {})
    assert grantee is not None
    assert grantee.get("Type") == "CanonicalUser"
    # Ensure the grantee matches the owner reported by the ACL
    assert grantee.get("ID") == owner.get("ID")

def test_put_acl_returns_ok_get_acl_returns_the_acl(s3_client, bucket_name: str):
    source_key = UPLOAD_FILE_NAME

    # Create bucket that allows setting ACLs and put an object
    s3_client.create_bucket(Bucket=bucket_name, ObjectOwnership="ObjectWriter")
    given_object(s3_client, bucket_name, source_key)

    user_id = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ab"
    user_name = "John Doe"
    grantee_id = "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2ef"
    grantee_name = "Jane Doe"

    # Put explicit ACL with owner and a single FULL_CONTROL grant to a grantee
    put_resp = s3_client.put_object_acl(
        Bucket=bucket_name,
        Key=source_key,
        AccessControlPolicy={
            "Owner": {
                "ID": user_id,
                "DisplayName": user_name,
            },
            "Grants": [
                {
                    "Permission": "FULL_CONTROL",
                    "Grantee": {
                        "Type": "CanonicalUser",
                        "ID": grantee_id,
                        "DisplayName": grantee_name,
                    },
                }
            ],
        },
    )
    assert 200 <= put_resp["ResponseMetadata"]["HTTPStatusCode"] < 300

    # Get ACL and verify it matches what we set
    acl_resp = s3_client.get_object_acl(Bucket=bucket_name, Key=source_key)
    assert 200 <= acl_resp["ResponseMetadata"]["HTTPStatusCode"] < 300

    owner = acl_resp.get("Owner", {})
    assert owner is not None
    assert owner.get("ID") == user_id

    grants = acl_resp.get("Grants", [])
    assert len(grants) == 1

    grant = grants[0]
    assert grant.get("Permission") == "FULL_CONTROL"

    grantee = grant.get("Grantee", {})
    assert grantee is not None
    assert grantee.get("ID") == grantee_id
    assert grantee.get("DisplayName") == grantee_name
    assert grantee.get("Type") == "CanonicalUser"
