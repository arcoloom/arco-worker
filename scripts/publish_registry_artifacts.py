from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import subprocess
import tempfile
from pathlib import Path


DEFAULT_BUCKET = "arco-registry"
DEFAULT_BASE_URL = "https://registry.arcoloom.com"
DEFAULT_CHANNELS = ("latest", "stable")
ARTIFACT_KIND = "worker"
ARTIFACT_NAME = "arco-worker"
ARTIFACT_OS = "linux"
ARTIFACT_FILES = {
    "amd64": "arco-worker-linux-amd64",
    "arm64": "arco-worker-linux-arm64",
}


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
        default=os.environ.get("ARCO_REGISTRY_BUCKET", DEFAULT_BUCKET),
        help="Registry R2 bucket name.",
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
    return parser


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def upload_object(bucket: str, key: str, source: Path) -> None:
    subprocess.run(
        [
            "npx",
            "wrangler@4",
            "r2",
            "object",
            "put",
            f"{bucket}/{key}",
            "--file",
            str(source),
        ],
        check=True,
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


def main() -> None:
    args = build_parser().parse_args()
    version = args.version.strip()
    if not version:
        raise SystemExit("--version is required")

    base_url = args.base_url.rstrip("/")
    channels = tuple(dict.fromkeys(args.channels or DEFAULT_CHANNELS))
    dist_dir: Path = args.dist_dir

    descriptors: dict[str, dict[str, object]] = {}
    for arch, filename in ARTIFACT_FILES.items():
        path = dist_dir / filename
        if not path.is_file():
            raise SystemExit(f"artifact file not found: {path}")
        key = f"artifacts/{ARTIFACT_KIND}/{ARTIFACT_NAME}/{version}/{ARTIFACT_OS}/{arch}/{filename}"
        upload_object(args.bucket, key, path)
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
                    args.bucket,
                    f"manifests/channels/artifacts/{ARTIFACT_KIND}/{ARTIFACT_NAME}/{channel}/{ARTIFACT_OS}/{arch}.json",
                    channel_manifest_path,
                )
            finally:
                channel_manifest_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
