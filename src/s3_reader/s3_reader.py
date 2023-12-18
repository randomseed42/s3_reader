import json
import os
from concurrent.futures import as_completed, ThreadPoolExecutor
from functools import partial
from io import BytesIO
from typing import Callable, Literal

import boto3
from dotenv import load_dotenv
from mypy_boto3_s3 import S3Client
from tqdm import tqdm

load_dotenv()

AWS_PROFILE_NAME = os.getenv('AWS_PROFILE_NAME', 'default')
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME', 'cn-north-1')


def create_s3_client() -> S3Client:
    """
    Create an AWS S3 client.
    :return: client: S3Client
    """
    session = boto3.Session(profile_name=AWS_PROFILE_NAME, region_name=AWS_REGION_NAME)
    client: S3Client = session.client(service_name='s3', use_ssl=False, verify=False)
    return client


def list_prefixes_by_prefix(
        client: S3Client,
        bucket: str,
        prefix: str,
) -> list[str]:
    """
    List prefixes (next level folders) in a S3 bucket with a single file prefix, each key is a file's full path.
    Each list_objects_v2 request returns max 1000 keys, so use a while loop to get all keys under the prefix.
    :param client: S3Client
    :param bucket: AWS S3 Bucket
    :param prefix: AWS S3 file prefix
    :return: list of AWS S3 prefixes' full path
    """
    try:
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        prefixes: list[str] = [common_prefixes.get('Prefix') for common_prefixes in resp.get('CommonPrefixes', [])]

        while continuation_token := resp.get('NextContinuationToken'):
            resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/',
                                          ContinuationToken=continuation_token)
            prefixes.extend(common_prefixes.get('Prefix') for common_prefixes in resp.get('CommonPrefixes', []))
        return prefixes
    except Exception as err:
        print(f'Error {err}')
        return []


def list_prefixes_by_prefixes(
        bucket: str,
        prefixes: list[str],
) -> list[str]:
    """
    List prefixes (next level folders) in a S3 bucket with multiple file prefixes.
    Use concurrent with max 16 workers to speed up.
    Reuse same client to avoid cost of create and close a client.
    :param bucket: AWS S3 Bucket
    :param prefixes: list of AWS S3 file prefix
    :return: list of AWS S3 prefixes' full path
    """
    if len(prefixes) > 0:
        client = create_s3_client()
        try:
            func: Callable = partial(list_prefixes_by_prefix, client, bucket)
            max_workers = 16
            sub_level_prefixes = []

            with ThreadPoolExecutor(min(max_workers, len(prefixes))) as executor:
                jobs = [executor.submit(func, prefix) for prefix in prefixes]
                jobs_iter = tqdm(as_completed(jobs), total=len(prefixes), desc='List prefixes by prefixes')
                for job in jobs_iter:
                    sub_level_prefixes.extend(job.result())
        finally:
            client.close()
        return sub_level_prefixes


def list_keys_by_prefix(
        client: S3Client,
        bucket: str,
        prefix: str,
) -> list[str]:
    """
    List keys in a S3 bucket with a single file prefix, each key is a file's full path.
    Each list_objects_v2 request returns max 1000 keys, so use a while loop to get all keys under the prefix.
    :param client: S3Client
    :param bucket: AWS S3 Bucket
    :param prefix: AWS S3 file prefix
    :return: list of AWS S3 files' full path
    """
    try:
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys: list[str] = [content.get('Key') for content in resp.get('Contents', [])]

        while continuation_token := resp.get('NextContinuationToken'):
            resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
            keys.extend(content.get('Key') for content in resp.get('Contents', []))
        return keys
    except Exception as err:
        print(f'Error {err}')
        return []


def list_keys_by_prefixes(
        bucket: str,
        prefixes: list[str],
) -> list[str]:
    """
    List keys in a S3 bucket with multiple file prefixes.
    Use concurrent with max 16 workers to speed up.
    Reuse same client to avoid cost of create and close a client.
    :param bucket: AWS S3 Bucket
    :param prefixes: list of AWS S3 file prefix
    :return: list of AWS S3 files' full path
    """
    if len(prefixes) > 0:
        client = create_s3_client()
        try:
            func: Callable = partial(list_keys_by_prefix, client, bucket)
            max_workers = 16
            keys = []

            with ThreadPoolExecutor(min(max_workers, len(prefixes))) as executor:
                jobs = [executor.submit(func, prefix) for prefix in prefixes]
                jobs_iter = tqdm(as_completed(jobs), total=len(prefixes), desc='List keys by prefixes')
                for job in jobs_iter:
                    keys.extend(job.result())
        finally:
            client.close()
        return keys


def download_by_key(
        client: S3Client,
        bucket: str,
        key: str,
        output_format: Literal['dict', 'bytes'] = 'dict',
        output_dir: str | None = '',
) -> dict | None:
    """
    Download a single AWS S3 file per the bucket and key (file full path).
    Return a dict which key is the file full path and value is the data in the file.
    Choose the value format as dict of the data, or the bytes of the data.
    :param client: AWS S3 Bucket
    :param bucket: AWS S3 Bucket
    :param key: AWS S3 file's full path
    :param output_format: dict or bytes
    :param output_dir: write to a directory if not None
    :return: {key: json.loads(data)} or {key: data}
    """
    try:
        with BytesIO() as buf:
            client.download_fileobj(Bucket=bucket, Key=key, Fileobj=buf)
            buf.seek(0)
            data = buf.read()
            if output_format == 'dict':
                return {key: json.loads(data)}
            elif output_format == 'bytes':
                if output_dir is not None:
                    os.makedirs(os.path.join(output_dir, os.path.dirname(key)), exist_ok=True)
                    with open(os.path.join(output_dir, key), 'wb') as f:
                        f.write(data)
                return {key: data}
            else:
                raise ValueError('output_format must be "dict" or "bytes"')
    except Exception as err:
        print(err)
        return


def download_by_keys(
        bucket: str,
        keys: list[str],
        output_format: Literal['dict', 'bytes'] = 'dict',
        output_dir: str | None = '',
) -> dict[str, dict | bytes]:
    """
    Download many AWS S3 files per the bucket and keys (file full path).
    Return a list of dict, for each item the key is the file full path and value is the data in the file.
    Choose the value format as dict of the data, or the bytes of the data.
    :param bucket: AWS S3 Bucket
    :param keys: list of AWS S3 file paths
    :param output_format: dict or bytes
    :param output_dir: write to a directory if not None
    :return: list of {key: data}
    """
    if len(keys) > 0:
        client = create_s3_client()
        try:
            func: Callable = partial(download_by_key, client, bucket,
                                     output_format=output_format, output_dir=output_dir)
            max_workers = 16
            data = {}

            with ThreadPoolExecutor(min(max_workers, len(keys))) as executor:
                jobs = [executor.submit(func, key) for key in keys]
                jobs_iter = tqdm(as_completed(jobs), total=len(keys), desc='Download file by keys')
                for job in jobs_iter:
                    data.update(job.result())
        finally:
            client.close()
        return data


def download_by_prefixes(
        bucket: str,
        prefixes: list[str],
        output_format: Literal['dict', 'bytes'] = 'dict',
        output_dir: str | None = '',
) -> dict[str, dict | bytes]:
    """
    Download many AWS S3 files per the bucket and prefixes.
    First get the keys as a list, then download the keys and return a dict with {key: data},
    data format is decided by output_format.
    If output_format is bytes, the data will be written in a folder
    per the dirname of the key under current working directory.
    :param bucket: AWS S3 Bucket
    :param prefixes: list of AWS S3 file prefix
    :param output_format: dict or bytes
    :param output_dir: write to a directory if not None
    :return: dict[]
    """
    keys = list_keys_by_prefixes(bucket=bucket, prefixes=prefixes)
    data = download_by_keys(bucket=bucket, keys=keys, output_format=output_format, output_dir=output_dir)
    return data
