import json
from datetime import datetime, timezone
from typing import Any

import boto3

s3 = boto3.client("s3")
cloudfront = boto3.client("cloudfront")


def put_json(bucket: str, key: str, data: Any) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data))


def read_json(bucket: str, key: str) -> Any:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as err:
        print(f"[storage] error reading s3://{bucket}/{key}: {err}")
        raise


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


def list_objects(bucket: str, prefix: str) -> list[dict]:
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return resp.get("Contents", [])
    except Exception as err:
        print(f"[storage] failed to list s3://{bucket}/{prefix}: {err}")
        return []


def delete_object(bucket: str, key: str) -> None:
    try:
        s3.delete_object(Bucket=bucket, Key=key)
    except Exception as err:
        print(f"[storage] failed to delete s3://{bucket}/{key}: {err}")


def invalidate_cache(dist_id: str, paths: list[str]) -> None:
    cloudfront.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={
            "Paths": {"Quantity": len(paths), "Items": paths},
            "CallerReference": datetime.now(timezone.utc).isoformat(),
        },
    )
