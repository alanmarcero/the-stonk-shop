import json
from datetime import datetime, timezone
from typing import Any

# We import inside functions to support test mocking of app.s3/cloudfront
# until we can refactor tests to mock storage.s3/cloudfront.

def _get_s3():
    from . import app
    return app.s3

def _get_cloudfront():
    from . import app
    return app.cloudfront


def put_json(bucket: str, key: str, data: Any) -> None:
    _get_s3().put_object(Bucket=bucket, Key=key, Body=json.dumps(data))


def read_json(bucket: str, key: str) -> Any:
    try:
        resp = _get_s3().get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read())
    except Exception as err:
        print(f"[storage] failed to read s3://{bucket}/{key}: {err}")
        return None


def list_objects(bucket: str, prefix: str) -> list[dict]:
    try:
        resp = _get_s3().list_objects_v2(Bucket=bucket, Prefix=prefix)
        return resp.get("Contents", [])
    except Exception as err:
        print(f"[storage] failed to list s3://{bucket}/{prefix}: {err}")
        return []


def delete_object(bucket: str, key: str) -> None:
    try:
        _get_s3().delete_object(Bucket=bucket, Key=key)
    except Exception as err:
        print(f"[storage] failed to delete s3://{bucket}/{key}: {err}")


def invalidate_cache(dist_id: str, paths: list[str]) -> None:
    _get_cloudfront().create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={
            "Paths": {"Quantity": len(paths), "Items": paths},
            "CallerReference": datetime.now(timezone.utc).isoformat(),
        },
    )
