from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import tempfile
from pathlib import Path

try:
    import boto3
    from botocore.config import Config
except ImportError as exc:  # pragma: no cover - surfaced as a CLI error
    raise SystemExit("boto3 is required to publish registry artifacts. Install it with `pip install boto3`.") from exc


DEFAULT_BUCKET = "arco-registry"
DEFAULT_BASE_URL = "https://registry.arcoloom.com"
DEFAULT_CHANNELS = ("latest", "stable")
DEFAULT_S3_REGION = "auto"
ARTIFACT_KIND = "worker"
ARTIFACT_NAME = "arco-worker"
ARTIFACT_OS = "linux"
ARTIFACT_FILES = {
    "amd64": "arco-worker-linux-amd64",
    "arm64": "arco-worker-linux-arm64",
}


def first_env(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish arco-worker release artifacts and manifests to the registry bucket."
    )
    parser.add_argument("--version", required=True, help="Immutable worker version.")
    parser.add_argument(
        "--dist-dir",
        type=Path,
        default=Path("dist"),
        help="Directory containing built release artifacts.",
    )
    parser.add_argument(
        "--bucket",
        default=first_env("S3_BUCKET") or DEFAULT_BUCKET,
        help="S3 bucket name.",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("ARCO_REGISTRY_BASE_URL", DEFAULT_BASE_URL),
        help="Public registry base URL.",
    )
    parser.add_argument(
        "--channel",
        dest="channels",
        action="append",
        default=[],
        help="Channel name to update. Can be provided multiple times.",
    )
    parser.add_argument(
        "--endpoint-url",
        help="S3-compatible endpoint URL. Defaults to S3_ENDPOINT.",
    )
    parser.add_argument(
        "--region",
        default=first_env("S3_REGION") or DEFAULT_S3_REGION,
        help="S3 region name.",
    )
    return parser


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def resolve_endpoint_url(explicit: str | None) -> str:
    endpoint_url = explicit or first_env("S3_ENDPOINT")
    if endpoint_url and endpoint_url.strip():
        return endpoint_url.rstrip("/")

    raise SystemExit(
        "S3 endpoint not configured. Pass --endpoint-url or set "
        "S3_ENDPOINT."
    )


def create_s3_client(endpoint_url: str, region: str):
    access_key_id = first_env("S3_ACCESS_KEY_ID")
    secret_access_key = first_env("S3_SECRET_ACCESS_KEY")
    session_token = first_env("S3_SESSION_TOKEN")

    client_kwargs = {
        "endpoint_url": endpoint_url,
        "region_name": region,
        "config": Config(
            retries={"max_attempts": 10, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    }
    if access_key_id:
        client_kwargs["aws_access_key_id"] = access_key_id
    if secret_access_key:
        client_kwargs["aws_secret_access_key"] = secret_access_key
    if session_token:
        client_kwargs["aws_session_token"] = session_token

    return boto3.client(
        "s3",
        **client_kwargs,
    )


def write_json_temp(payload: dict[str, object]) -> Path:
    handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False)
    with handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return Path(handle.name)


def content_type_for(path: Path) -> str:
    guessed, _ = mimetypes.guess_type(path.name)
    return guessed or "application/octet-stream"


def upload_object(client, bucket: str, key: str, source: Path) -> None:
    client.upload_file(
        str(source),
        bucket,
        key,
        ExtraArgs={
            "ContentType": content_type_for(source),
        },
    )


def main() -> None:
    args = build_parser().parse_args()
    version = args.version.strip()
    if not version:
        raise SystemExit("--version is required")

    base_url = args.base_url.rstrip("/")
    channels = tuple(dict.fromkeys(args.channels or DEFAULT_CHANNELS))
    dist_dir: Path = args.dist_dir
    endpoint_url = resolve_endpoint_url(args.endpoint_url)
    s3_client = create_s3_client(endpoint_url, args.region)

    descriptors: dict[str, dict[str, object]] = {}
    for arch, filename in ARTIFACT_FILES.items():
        path = dist_dir / filename
        if not path.is_file():
            raise SystemExit(f"artifact file not found: {path}")
        key = f"artifacts/{ARTIFACT_KIND}/{ARTIFACT_NAME}/{version}/{ARTIFACT_OS}/{arch}/{filename}"
        upload_object(s3_client, args.bucket, key, path)
        descriptors[arch] = {
            "key": key,
            "filename": filename,
            "sha256": sha256_file(path),
            "size": path.stat().st_size,
            "content_type": content_type_for(path),
        }

        version_manifest = {
            "schema": "arco.artifact.version.v1",
            "artifact": {
                "kind": ARTIFACT_KIND,
                "name": ARTIFACT_NAME,
                "version": version,
                "os": ARTIFACT_OS,
                "arch": arch,
                "filename": filename,
            },
            "object": descriptors[arch],
            "metadata": {
                "source": "arco-worker",
            },
        }
        version_manifest_path = write_json_temp(version_manifest)
        try:
            upload_object(
                s3_client,
                args.bucket,
                f"manifests/versions/artifacts/{ARTIFACT_KIND}/{ARTIFACT_NAME}/{version}/{ARTIFACT_OS}/{arch}.json",
                version_manifest_path,
            )
        finally:
            version_manifest_path.unlink(missing_ok=True)

    for channel in channels:
        for arch, filename in ARTIFACT_FILES.items():
            channel_manifest = {
                "schema": "arco.artifact.channel.v1",
                "channel": channel,
                "artifact": {
                    "kind": ARTIFACT_KIND,
                    "name": ARTIFACT_NAME,
                    "version": version,
                    "os": ARTIFACT_OS,
                    "arch": arch,
                    "filename": filename,
                },
                "object": descriptors[arch],
                "metadata": {
                    "source": "arco-worker",
                    "channel": channel,
                    "base_url": base_url,
                },
            }
            channel_manifest_path = write_json_temp(channel_manifest)
            try:
                upload_object(
                    s3_client,
                    args.bucket,
                    f"manifests/channels/artifacts/{ARTIFACT_KIND}/{ARTIFACT_NAME}/{channel}/{ARTIFACT_OS}/{arch}.json",
                    channel_manifest_path,
                )
            finally:
                channel_manifest_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
