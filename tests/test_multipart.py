import os
import tempfile

import pytest
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import CompletedPartTypeDef

from s3mock_test import (
    UPLOAD_FILE_LENGTH,
    UPLOAD_FILE_NAME,
    crc32_b64,
    crc64nvme_b64,
    given_bucket,
    hex_digest,
    multipart_crc32_checksum,
    multipart_etag_hex,
    upload_file_bytes,
)

# Reimplementation of https://github.com/adobe/S3Mock/blob/main/integration-tests/src/test/kotlin/com/adobe/testing/s3mock/its/MultipartIT.kt

def test_multipart_upload_with_crc32_checksum(
    transfer_manager, s3_client: S3Client, bucket_name: str
):
    # Arrange
    given_bucket(s3_client, bucket_name)
    file_path = UPLOAD_FILE_NAME
    key = UPLOAD_FILE_NAME
    body = upload_file_bytes()
    expected_crc32 = crc32_b64(body)
    expected_length = UPLOAD_FILE_LENGTH

    # Act: multipart upload using transfer_manager
    future = transfer_manager.upload(
        file_path,
        bucket_name,
        key,
        extra_args={"ChecksumAlgorithm": "CRC32"},
    )
    future.result()

    # Assert: ChecksumCRC32 in head_object response
    head = s3_client.head_object(
        Bucket=bucket_name, Key=key, ChecksumMode="ENABLED"
    )
    assert head.get("ChecksumCRC32") == expected_crc32

    # Wait for object to exist
    s3_client.get_waiter("object_exists").wait(Bucket=bucket_name, Key=key)

    # Compute hex digest of uploaded file
    upload_digest = hex_digest(file_path)

    # Download the object to a temp file and compare
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with open(tmp_path, "wb") as out:
            resp = s3_client.get_object(Bucket=bucket_name, Key=key)
            data = resp["Body"].read()
            out.write(data)
        assert os.path.getsize(tmp_path) == expected_length
        with open(file_path, "rb") as orig, open(tmp_path, "rb") as down:
            assert orig.read() == down.read()
        downloaded_digest = hex_digest(tmp_path)
        assert upload_digest == downloaded_digest
    finally:
        os.unlink(tmp_path)


def test_multipart_upload_and_download_transfer_manager(
    transfer_manager, s3_client: S3Client, bucket_name: str
):
    # Arrange
    given_bucket(s3_client, bucket_name)
    file_path = UPLOAD_FILE_NAME
    key = UPLOAD_FILE_NAME
    expected_length = UPLOAD_FILE_LENGTH

    # Upload file using transfer_manager
    future = transfer_manager.upload(file_path, bucket_name, key)
    future.result()

    # Verify with get_object
    resp = s3_client.get_object(Bucket=bucket_name, Key=key)
    content = resp["Body"].read()
    assert len(content) == expected_length

    # Download file using transfer_manager
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name
    try:
        download_future = transfer_manager.download(bucket_name, key, tmp_path)
        download_future.result()
        # Check downloaded file size
        assert os.path.getsize(tmp_path) == expected_length
        # Check binary content matches original
        with open(file_path, "rb") as orig, open(tmp_path, "rb") as down:
            assert orig.read() == down.read()
    finally:
        os.unlink(tmp_path)


def test_multipart_upload_with_user_metadata(s3_client: S3Client, bucket_name: str):
    # Arrange
    given_bucket(s3_client, bucket_name)
    file_path = UPLOAD_FILE_NAME
    key = UPLOAD_FILE_NAME
    expected_length = UPLOAD_FILE_LENGTH
    user_metadata = {"key": "value"}

    # Initiate multipart upload with metadata
    resp = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        Metadata=user_metadata,
    )
    upload_id = resp["UploadId"]

    # Upload part 1
    with open(file_path, "rb") as f:
        upload_part_resp = s3_client.upload_part(
            Bucket=bucket_name,
            Key=key,
            UploadId=upload_id,
            PartNumber=1,
            ContentLength=expected_length,
            Body=f,
        )
    etag = upload_part_resp["ETag"]

    # Complete multipart upload
    s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={
            "Parts": [
                CompletedPartTypeDef(ETag=etag, PartNumber=1)
            ]
        },
    )

    # Get object and verify metadata
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    returned_md = obj.get("Metadata", {})
    # S3 lowercases user metadata keys
    assert returned_md.get("key") == "value"


def test_multipart_upload_with_checksum_type_composite(
    s3_client: S3Client, bucket_name: str
):
    # Arrange
    given_bucket(s3_client, bucket_name)
    body = upload_file_bytes()
    parts = [body]
    expected_length = len(body)
    expected_crc32 = multipart_crc32_checksum(parts)
    expected_etag = f'"{multipart_etag_hex(parts)}"'

    # Initiate multipart upload with checksum settings
    resp = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=UPLOAD_FILE_NAME,
        ChecksumAlgorithm="CRC32",
        ChecksumType="COMPOSITE",
    )
    upload_id = resp["UploadId"]

    # Upload a single part with checksum algorithm enabled
    upload_part_resp = s3_client.upload_part(
        Bucket=bucket_name,
        Key=UPLOAD_FILE_NAME,
        UploadId=upload_id,
        PartNumber=1,
        ChecksumAlgorithm="CRC32",
        ContentLength=expected_length,
        Body=body,
    )
    part_etag = upload_part_resp["ETag"]
    part_checksum = upload_part_resp.get("ChecksumCRC32")
    assert part_checksum, "UploadPart response should include ChecksumCRC32"

    # Complete multipart upload with composite checksum type
    complete_resp = s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=UPLOAD_FILE_NAME,
        UploadId=upload_id,
        ChecksumType="COMPOSITE",
        MultipartUpload={
            "Parts": [
                CompletedPartTypeDef(
                    ETag=part_etag,
                    PartNumber=1,
                    ChecksumCRC32=part_checksum,
                )
            ]
        },
    )
    assert complete_resp.get("ChecksumCRC32") == expected_crc32

    # Fetch the object with checksum mode enabled and verify values
    get_resp = s3_client.get_object(
        Bucket=bucket_name,
        Key=UPLOAD_FILE_NAME,
        ChecksumMode="ENABLED",
    )
    assert get_resp["ETag"] == expected_etag
    assert get_resp.get("ChecksumCRC32") == expected_crc32


def test_multipart_upload_with_checksum_type_full_object(
    s3_client: S3Client, bucket_name: str
):
    # Arrange
    given_bucket(s3_client, bucket_name)
    body = upload_file_bytes()
    parts = [body]
    expected_length = len(body)
    # FULL_OBJECT returns the checksum of the assembled object, not multipart composite form.
    expected_checksum = crc64nvme_b64(body)
    expected_etag = f'"{multipart_etag_hex(parts)}"'

    # Initiate multipart upload with FULL_OBJECT checksum type
    resp = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=UPLOAD_FILE_NAME,
        ChecksumAlgorithm="CRC64NVME",
        ChecksumType="FULL_OBJECT",
    )
    upload_id = resp["UploadId"]

    # Upload part with CRC64NVME algorithm
    upload_part_resp = s3_client.upload_part(
        Bucket=bucket_name,
        Key=UPLOAD_FILE_NAME,
        UploadId=upload_id,
        PartNumber=1,
        ChecksumAlgorithm="CRC64NVME",
        ContentLength=expected_length,
        Body=body,
    )
    part_etag = upload_part_resp["ETag"]
    part_checksum = upload_part_resp.get("ChecksumCRC64NVME")
    assert part_checksum, "UploadPart response should include ChecksumCRC64NVME"
    assert part_checksum == crc64nvme_b64(body)

    # Complete multipart upload expecting a full-object checksum
    complete_resp = s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=UPLOAD_FILE_NAME,
        UploadId=upload_id,
        ChecksumType="FULL_OBJECT",
        MultipartUpload={
            "Parts": [
                CompletedPartTypeDef(
                    ETag=part_etag,
                    PartNumber=1,
                    ChecksumCRC64NVME=part_checksum,
                )
            ]
        },
    )
    assert complete_resp.get("ChecksumCRC64NVME") == expected_checksum

    # Verify via GetObject with checksum mode enabled
    get_resp = s3_client.get_object(
        Bucket=bucket_name,
        Key=UPLOAD_FILE_NAME,
        ChecksumMode="ENABLED",
    )
    assert get_resp["ETag"] == expected_etag
    assert get_resp.get("ChecksumCRC64NVME") == expected_checksum


def test_multipart_upload_with_checksum_type_mismatch_raises(
    s3_client: S3Client, bucket_name: str
):
    # Arrange
    given_bucket(s3_client, bucket_name)
    body = upload_file_bytes()
    expected_length = len(body)
    full_object_checksum = crc32_b64(body)

    resp = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=UPLOAD_FILE_NAME,
        ChecksumAlgorithm="CRC32",
        ChecksumType="COMPOSITE",
    )
    upload_id = resp["UploadId"]

    upload_part_resp = s3_client.upload_part(
        Bucket=bucket_name,
        Key=UPLOAD_FILE_NAME,
        UploadId=upload_id,
        PartNumber=1,
        ChecksumAlgorithm="CRC32",
        ContentLength=expected_length,
        Body=body,
    )
    part_etag = upload_part_resp["ETag"]
    part_checksum = upload_part_resp.get("ChecksumCRC32")
    assert part_checksum, "UploadPart response should include ChecksumCRC32"

    with pytest.raises(ClientError) as exc:
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=UPLOAD_FILE_NAME,
            UploadId=upload_id,
            ChecksumType="FULL_OBJECT",
            ChecksumCRC32=full_object_checksum,
            MultipartUpload={
                "Parts": [
                    CompletedPartTypeDef(
                        ETag=part_etag,
                        PartNumber=1,
                        ChecksumCRC32=part_checksum,
                    )
                ]
            },
        )

    err = exc.value.response
    status = err.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status == 400

