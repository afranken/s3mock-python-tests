import base64
import datetime as dt
import hashlib
import os
import re
import time
import uuid
import zlib
from enum import Enum
from pathlib import Path
from typing import Optional
from urllib.parse import unquote as _url_unquote

import boto3
import pytest
from _pytest.fixtures import FixtureRequest
from awscrt import checksums as awscrt_checksums
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.paginate import PageIterator
from mypy_boto3_s3.client import S3Client
from mypy_boto3_s3.type_defs import (
    CreateBucketOutputTypeDef,
    ListMultipartUploadsRequestTypeDef,
    ListObjectVersionsOutputTypeDef,
    PutObjectOutputTypeDef,
)
from s3transfer.manager import TransferManager
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

UPLOAD_FILE_NAME = 'testfile.txt'
UPLOAD_FILE_LENGTH = Path(UPLOAD_FILE_NAME).stat().st_size
REGION = os.getenv("AWS_REGION", "us-east-1")
ONE_MB = 1024 * 1024
PREFIX = "prefix"
PAYLOAD_MAX_SIZE = "100MB"

container = (
    DockerContainer("adobe/s3mock:4.12.4")
    .with_exposed_ports(9090, 9191)
    .with_env("debug", "true")
    # Increase various limits to allow large payload testing, see test_presigned_urls.py
    .with_env("SERVER_TOMCAT_MAX_PART_COUNT", "-1")
    .with_env("SERVER_TOMCAT_MAX_SWALLOW_SIZE", PAYLOAD_MAX_SIZE)
    .with_env("SERVER_TOMCAT_MAX_HTTP_FORM_POST_SIZE", PAYLOAD_MAX_SIZE)
    .with_env("SPRING_SERVLET_MULTIPART_MAX_FILE_SIZE", PAYLOAD_MAX_SIZE)
    .with_env("SPRING_SERVLET_MULTIPART_MAX_REQUEST_SIZE", PAYLOAD_MAX_SIZE)
    .with_env("COM_ADOBE_TESTING_S3MOCK_DOMAIN_INITIAL_BUCKETS", "bucket-a, bucket-b")
    .with_env("COM_ADOBE_TESTING_S3MOCK_DOMAIN_VALID_KMS_KEYS",
              "arn:aws:kms:us-east-1:1234567890:key/valid-test-key-id")
)

# Constants used for S3 client configuration (moved from TestS3Mock to module scope)
_AWS_ACCESS_KEY = 'dummy-key'
_AWS_SECRET_ACCESS_KEY = 'dummy-key'
_AWS_SESSION_TOKEN = 'dummy-key'
_CONNECTION_TIMEOUT = 1
_READ_TIMEOUT = 60  # AWS default
_MAX_RETRIES = 3

@pytest.fixture(scope="function", autouse=True)
def test_name(request: FixtureRequest) -> str:
    # Prefer originalname; fall back to name if unavailable
    return str(getattr(request.node, "originalname", request.node.name))

# Bucket name max length is 63 characters.
# Truncate the test function name to 50 characters, plus a random suffix to avoid collisions
@pytest.fixture(scope="function", autouse=True)
def bucket_name(test_name: str) -> str:
    return f'{test_name[:50]}-{int(time.time())}'.replace('_', '-')

@pytest.fixture(autouse=True)
def cleanup(s3_client: S3Client):
    print("Setup")
    # currently, nothing to do here.
    yield
    print("Teardown")
    # clean up all resources created during the test
    buckets = s3_client.list_buckets()
    for bucket in buckets['Buckets']:
        if bucket['Name'] != 'bucket-a' and bucket['Name'] != 'bucket-b':
            name = bucket['Name']
            delete_multipart_uploads(s3_client, name)
            delete_objects_in_bucket(s3_client, name, object_lock_enabled=False)
            s3_client.delete_bucket(Bucket=name)

@pytest.fixture(scope="session", autouse=True)
def s3mock_container():
    # Start the container once per test session; Ryuk will stop it afterward
    container.waiting_for(LogMessageWaitStrategy(re.compile(r'.*Started S3MockApplication.*')))
    # for testing against a locally running S3Mock, do not start container here:
    env_endpoint = os.getenv("S3MOCK_ENDPOINT")
    if not env_endpoint:
        container.start()

@pytest.fixture(scope="session")
def endpoint_url(s3mock_container) -> str:
    # Allow overriding via environment variable for testing against a locally running S3Mock
    env_endpoint = os.getenv("S3MOCK_ENDPOINT")
    if env_endpoint:
        return env_endpoint
    ip = container.get_container_host_ip()
    port = container.get_exposed_port(9191)
    return f'https://{ip}:{port}'

@pytest.fixture(scope="session")
def endpoint_url_http(s3mock_container) -> str:
    # Allow overriding via environment variable for testing against a locally running S3Mock
    env_endpoint = os.getenv("S3MOCK_ENDPOINT_HTTP")
    if env_endpoint:
        return env_endpoint
    ip = container.get_container_host_ip()
    port = container.get_exposed_port(9090)
    return f'http://{ip}:{port}'

@pytest.fixture(scope="session", autouse=True)
def s3_client(endpoint_url) -> S3Client:
    config = Config(
        connect_timeout=_CONNECTION_TIMEOUT,
        read_timeout=_READ_TIMEOUT,
        retries={'max_attempts': _MAX_RETRIES},
        signature_version='s3v4',
        s3={'addressing_style': 'path'},
        max_pool_connections=100,
    )
    return boto3.client(
        's3',
        aws_access_key_id=_AWS_ACCESS_KEY,
        aws_secret_access_key=_AWS_SECRET_ACCESS_KEY,
        aws_session_token=_AWS_SESSION_TOKEN,
        config=config,
        endpoint_url=endpoint_url,
        verify=False,  # Skip SSL certificate verification (use only in tests)
    )

@pytest.fixture(scope="session", autouse=True)
def transfer_manager(s3_client: S3Client) -> TransferManager:
    """
    Create a Transfer Manager equivalent with multipart enabled and high concurrency.
    """
    transfer_config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,  # 8 MiB threshold for multipart
        multipart_chunksize=8 * 1024 * 1024,  # 8 MiB parts
        max_concurrency=100,                  # similar to CRT maxConcurrency
        use_threads=True,                     # parallel uploads/downloads
    )
    return TransferManager(s3_client, config=transfer_config)

@pytest.fixture(scope="session", autouse=True)
def s3_client_http(endpoint_url_http) -> S3Client:
    config = Config(
        connect_timeout=_CONNECTION_TIMEOUT,
        read_timeout=_READ_TIMEOUT,
        retries={'max_attempts': _MAX_RETRIES},
        signature_version='s3v4',
        s3={'addressing_style': 'path'},
        max_pool_connections=100,
    )
    return boto3.client(
        's3',
        aws_access_key_id=_AWS_ACCESS_KEY,
        aws_secret_access_key=_AWS_SECRET_ACCESS_KEY,
        aws_session_token=_AWS_SESSION_TOKEN,
        config=config,
        endpoint_url=endpoint_url_http,
    )

def upload_file_bytes() -> bytes:
    with open('testfile.txt', 'rb') as file:
        return file.read()

def delete_multipart_uploads(s3_client: S3Client, bucket_name: str) -> None:
    """
    Abort all in-progress multipart uploads in the specified bucket.
    Mirrors the behavior of the provided Kotlin snippet.
    """
    key_marker: Optional[str] = None
    upload_id_marker: Optional[str] = None

    while True:
        params: ListMultipartUploadsRequestTypeDef = {"Bucket": bucket_name}
        if key_marker is not None:
            params["KeyMarker"] = key_marker
        if upload_id_marker is not None:
            params["UploadIdMarker"] = upload_id_marker

        resp = s3_client.list_multipart_uploads(**params)

        for upload in (resp.get("Uploads") or []):
            s3_client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=upload["Key"],
                UploadId=upload["UploadId"],
            )

        if not resp.get("IsTruncated"):
            break

        key_marker = resp.get("NextKeyMarker")
        upload_id_marker = resp.get("NextUploadIdMarker")

def delete_objects_in_bucket(
        s3_client: S3Client,
        bucket_name: str,
        object_lock_enabled: bool
) -> None:
    """
    Delete all object versions and delete markers in the bucket.
    If object lock is enabled, clear any potential legal holds before deletion.
    """
    paginator = s3_client.get_paginator("list_object_versions")
    page_iterator: PageIterator[ListObjectVersionsOutputTypeDef] = paginator.paginate(
        Bucket=bucket_name,
        EncodingType="url",
    )

    for page in page_iterator:
        is_url_encoded = page.get("EncodingType") == "url"
        # Handle object versions
        for version in page.get("Versions", []) or []:
            key = _url_unquote(version["Key"]) if is_url_encoded else version["Key"]
            if object_lock_enabled:
                s3_client.put_object_legal_hold(
                    Bucket=bucket_name,
                    Key=key,
                    VersionId=version["VersionId"],
                    LegalHold={"Status": "OFF"},
                )
            s3_client.delete_object(
                Bucket=bucket_name,
                Key=key,
                VersionId=version["VersionId"],
            )

        # Handle delete markers
        for marker in page.get("DeleteMarkers", []) or []:
            key = _url_unquote(marker["Key"]) if is_url_encoded else marker["Key"]
            if object_lock_enabled:
                s3_client.put_object_legal_hold(
                    Bucket=bucket_name,
                    Key=key,
                    VersionId=marker["VersionId"],
                    LegalHold={"Status": "OFF"},
                )
            s3_client.delete_object(
                Bucket=bucket_name,
                Key=key,
                VersionId=marker["VersionId"],
            )

def delete_bucket(
        s3_client: S3Client,
        bucket_name: str
) -> None:
    """
    Delete the bucket and wait until it no longer exists.
    """
    s3_client.delete_bucket(Bucket=bucket_name)

    # Wait until the bucket is confirmed deleted
    waiter = s3_client.get_waiter("bucket_not_exists")
    waiter.wait(Bucket=bucket_name)

    # Optional parity with the Kotlin snippet's assertion: verify it's gone
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        raise AssertionError("Bucket still exists after deletion")
    except ClientError:
        # Expected: head_bucket should fail if the bucket no longer exists
        pass

def given_bucket(
        s3_client: S3Client,
        bucket_name: str
) -> CreateBucketOutputTypeDef:
    bucket = s3_client.create_bucket(Bucket=bucket_name)
    s3_client.get_waiter("bucket_exists").wait(Bucket=bucket_name)
    return bucket

def given_object(
        s3_client: S3Client,
        bucket_name: str,
        object_name: str = UPLOAD_FILE_NAME,
        **kwargs
) -> PutObjectOutputTypeDef:
    return s3_client.put_object(
        Bucket=bucket_name,
        Key=object_name,
        Body=upload_file_bytes(),
        ** kwargs
    )

def compute_md5_etag(data: bytes) -> str:
    # S3 single-part ETag is the hex MD5 in quotes
    return f"\"{hashlib.md5(data).hexdigest()}\""


def compute_sha256_checksum_b64(data: bytes) -> str:
    # AWS returns base64-encoded checksum for SHA256
    return base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")

def random_name() -> str:
    return f"{uuid.uuid4().hex}"[:63]

def special_key() -> str:
    # Includes spaces, unicode, URL-reserved chars that require escaping
    return 'spécial key/with spaces & symbols?#[]@!$&\'()*+,;=.txt'

def now_utc() -> dt.datetime:
    # Use naive UTC timestamps as boto3 expects
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)

def chars_safe_alphanumeric() -> str:
    """
    Chars that are safe to use (alphanumeric).
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    """
    return (
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    )

def chars_safe_special() -> str:
    """
    Chars that are safe yet special.
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    """
    return "!-_.*'()"

def chars_special_handling() -> str:
    """
    Chars that might need special handling.
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    """
    return "&$@=;/:+ ,?"

def chars_special_handling_unicode() -> str:
    """
    Unicode chars that might need special handling (control chars).
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    """
    control = "".join(chr(c) for c in range(0x00, 0x20))
    return control + chr(0x7F)

def chars_to_avoid() -> str:
    """
    Chars to avoid.
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
    """
    return "\\{^}%`]\">[~<#|"

def chars_safe() -> list[str]:
    """
    Returns a list with two entries: prefixed alphanumeric and safe-special sets.
    """
    return [
        f"{PREFIX}{chars_safe_alphanumeric()}",
        f"{PREFIX}{chars_safe_special()}",
    ]

def chars_safe_key() -> str:
    """
    Returns a single string combining alphanumeric and safe-special sets with an optional prefix.
    """
    return f"{PREFIX}{chars_safe_alphanumeric()}{chars_safe_special()}"

def chars_special() -> list[str]:
    """
    Returns a list with chars that may need special handling (optionally prefixed).
    """
    return [
        f"{PREFIX}{chars_special_handling()}",
        # If needed, add unicode control chars:
        # f"{PREFIX}{chars_special_handling_unicode()}",
    ]

def chars_special_key() -> str:
    """
    Returns a single string with chars that may need special handling (optionally prefixed).
    """
    return f"{PREFIX}{chars_special_handling()}"

def chars_to_avoid_list() -> list[str]:
    """
    Returns a list with chars to avoid (optionally prefixed).
    """
    return [
        f"{PREFIX}{chars_to_avoid()}",
        # If needed, add unicode set variant:
        # f"{prefix}{chars_special_handling_unicode()}",
    ]

def chars_to_avoid_key() -> str:
    """
    Returns a single string with chars to avoid (optionally prefixed).
    """
    return f"{PREFIX}{chars_to_avoid()}"

# Reimplementation of the Kotlin selection of checksum algorithms.
class ChecksumAlgorithm(str, Enum):
    SHA256 = "SHA256"
    SHA1 = "SHA1"
    CRC32 = "CRC32"
    CRC32C = "CRC32C"
    CRC64NVME = "CRC64NVME"

def checksum_algorithms() -> list[ChecksumAlgorithm]:
    """
    Returns the set of checksum algorithms to test/use, mirroring the original selection.
    """
    return [
        ChecksumAlgorithm.SHA256,
        ChecksumAlgorithm.SHA1,
        ChecksumAlgorithm.CRC32,
        ChecksumAlgorithm.CRC32C,
        ChecksumAlgorithm.CRC64NVME,
    ]

def crc32(data: bytes) -> bytes:
    crc = zlib.crc32(data) & 0xFFFFFFFF
    return crc.to_bytes(4, byteorder="big")

def crc32_b64(data: bytes) -> str:
    return base64.b64encode(crc32(data)).decode("ascii")

def crc64nvme(data: bytes) -> bytes:
    checksum = awscrt_checksums.crc64nvme(data)
    return checksum.to_bytes(8, byteorder="big")

def crc64nvme_b64(data: bytes) -> str:
    return base64.b64encode(crc64nvme(data)).decode("ascii")

def hex_digest(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def multipart_etag_hex(parts: list[bytes]) -> str:
    digests = [hashlib.md5(part).digest() for part in parts]
    combined = hashlib.md5(b"".join(digests)).hexdigest()
    return f"{combined}-{len(parts)}"


def multipart_crc32_checksum(parts: list[bytes]) -> str:
    part_checksums = [
        crc32(part)
        for part in parts
    ]
    checksum_b64 = crc32_b64(b"".join(part_checksums))
    return f"{checksum_b64}-{len(parts)}"


def multipart_crc64nvme_checksum(parts: list[bytes]) -> str:
    part_checksums = [
        crc64nvme(part)
        for part in parts
    ]
    checksum_b64 = crc64nvme_b64(b"".join(part_checksums))
    return f"{checksum_b64}-{len(parts)}"
