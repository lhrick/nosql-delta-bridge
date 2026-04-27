"""Integration tests for Azure Blob Storage via Azurite emulator.

Requires Docker. Run with:
    pytest -m integration

Skipped automatically when Docker is unavailable or the Azurite image is missing.
"""
import base64
import hashlib
import hmac as hmac_mod
import json
import subprocess
import time
from datetime import datetime, timezone

import pytest

AZURITE_ACCOUNT = "devstoreaccount1"
# Key from Azurite 3.35.0 source — differs from the outdated "well-known" key in older docs
AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="  # noqa: E501
AZURITE_CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=http;"
    f"AccountName={AZURITE_ACCOUNT};"
    f"AccountKey={AZURITE_KEY};"
    f"BlobEndpoint=http://127.0.0.1:10000/{AZURITE_ACCOUNT};"
)
CONTAINER = "test"

# Storage options in object-store / delta-rs key format.
# Both WriterConfig and DeadLetterQueue accept this same dict.
STORAGE_OPTIONS = {
    "AZURE_STORAGE_ACCOUNT_NAME": AZURITE_ACCOUNT,
    "AZURE_STORAGE_ACCOUNT_KEY": AZURITE_KEY,
    "AZURE_STORAGE_USE_EMULATOR": "true",
}

CONTAINER_NAME = "nosql-delta-bridge-azurite-test"

_AZURITE_KEY_BYTES = base64.b64decode(AZURITE_KEY)


def _azurite_shared_key(method: str, container: str, resource_suffix: str = "") -> dict:
    """Return Authorization headers for a container-level Azurite request.

    Azurite path-style URLs include the account name in the path, so the
    canonicalized resource is /account/account/container (account appears twice).
    """
    import requests  # noqa: F401 — imported for side-effect check only
    date = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    cr = f"/{AZURITE_ACCOUNT}/{AZURITE_ACCOUNT}/{container}{resource_suffix}"
    sts = f"{method}\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:2019-02-02\n{cr}"
    sig = base64.b64encode(
        hmac_mod.new(_AZURITE_KEY_BYTES, sts.encode("utf-8"), hashlib.sha256).digest()
    ).decode()
    return {
        "x-ms-date": date,
        "x-ms-version": "2019-02-02",
        "Authorization": f"SharedKey {AZURITE_ACCOUNT}:{sig}",
    }


def _create_azurite_container(container: str) -> None:
    import requests
    headers = _azurite_shared_key("PUT", container, resource_suffix="\nrestype:container")
    headers["Content-Length"] = "0"
    resp = requests.put(
        f"http://127.0.0.1:10000/{AZURITE_ACCOUNT}/{container}?restype=container",
        headers=headers,
    )
    assert resp.status_code in (201, 409), (
        f"Failed to create container: {resp.status_code} {resp.text}"
    )


def _read_azurite_blob(container: str, blob_path: str) -> str:
    import requests
    date = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    cr = f"/{AZURITE_ACCOUNT}/{AZURITE_ACCOUNT}/{container}/{blob_path}"
    sts = f"GET\n\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{date}\nx-ms-version:2019-02-02\n{cr}"
    sig = base64.b64encode(
        hmac_mod.new(_AZURITE_KEY_BYTES, sts.encode("utf-8"), hashlib.sha256).digest()
    ).decode()
    resp = requests.get(
        f"http://127.0.0.1:10000/{AZURITE_ACCOUNT}/{container}/{blob_path}",
        headers={"x-ms-date": date, "x-ms-version": "2019-02-02",
                 "Authorization": f"SharedKey {AZURITE_ACCOUNT}:{sig}"},
    )
    assert resp.status_code == 200, f"Failed to read blob: {resp.status_code} {resp.text[:200]}"
    return resp.text


def _wait_for_azurite(timeout: int = 30) -> bool:
    import urllib.error
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:10000/{AZURITE_ACCOUNT}?comp=list", timeout=1)
            return True
        except urllib.error.HTTPError:
            return True  # any HTTP response means the service is up
        except Exception:
            time.sleep(0.5)
    return False


@pytest.fixture(scope="session")
def azurite():
    # remove any leftover container from a previous run
    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)

    proc = subprocess.run(
        [
            "docker", "run", "-d", "--name", CONTAINER_NAME,
            "-p", "10000:10000",
            "mcr.microsoft.com/azure-storage/azurite",
            "azurite-blob", "--blobHost", "0.0.0.0",
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        pytest.skip(f"Could not start Azurite container: {proc.stderr.strip()}")

    if not _wait_for_azurite():
        subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)
        pytest.skip("Azurite did not become healthy in time")

    # Create the blob container via raw HTTP + HMAC-SHA256.
    # adlfs/azure-storage-blob SDK auth fails against Azurite 3.35.0 because the
    # "well-known" Azurite key shipped in older docs changed in this version.
    # delta-rs (Rust object_store) authenticates correctly; here we replicate it.
    _create_azurite_container(CONTAINER)

    yield STORAGE_OPTIONS

    subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)


# ── writer ────────────────────────────────────────────────────────────────────

@pytest.mark.integration
def test_write_delta_table_to_azure(azurite):
    from nosql_delta_bridge.infer import infer_schema
    from nosql_delta_bridge.writer import WriterConfig, write_batch

    docs = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    schema = infer_schema(docs)

    config = WriterConfig(
        table_uri=f"az://{CONTAINER}/delta/users",
        source_collection="users",
        storage_options=azurite,
    )
    written = write_batch(docs, schema, config)
    assert written == 2


@pytest.mark.integration
def test_delta_table_append_to_azure(azurite):
    from nosql_delta_bridge.infer import infer_schema
    from nosql_delta_bridge.writer import WriterConfig, write_batch

    docs = [{"name": "Carol", "age": 28}]
    schema = infer_schema(docs)

    config = WriterConfig(
        table_uri=f"az://{CONTAINER}/delta/users",
        source_collection="users",
        storage_options=azurite,
    )
    written = write_batch(docs, schema, config)
    assert written == 1


# ── DLQ ──────────────────────────────────────────────────────────────────────

@pytest.mark.integration
def test_dlq_writes_to_azure(azurite):
    from nosql_delta_bridge.dlq import DeadLetterQueue

    dlq_uri = f"az://{CONTAINER}/dlq/rejected.ndjson"
    with DeadLetterQueue(dlq_uri, storage_options=azurite) as dlq:
        dlq.append(
            {"name": None, "age": 30},
            reason="null on non-nullable field 'name'",
            stage="coerce",
        )
        dlq.append({"name": "Bob", "age": "twenty"}, reason="cast failed on 'age'", stage="coerce")

    content = _read_azurite_blob(CONTAINER, "dlq/rejected.ndjson")
    lines = [json.loads(line) for line in content.splitlines()]

    assert len(lines) == 2
    assert lines[0]["reason"] == "null on non-nullable field 'name'"
    assert lines[1]["reason"] == "cast failed on 'age'"


# ── full pipeline via CLI ─────────────────────────────────────────────────────

@pytest.mark.integration
def test_cli_ingest_to_azure(azurite, tmp_path):
    from typer.testing import CliRunner

    from nosql_delta_bridge.cli import app

    input_file = tmp_path / "input.json"
    input_file.write_text(json.dumps([
        {"name": "Alice", "age": 30},
        {"name": "Bob",   "age": "twenty-five"},  # will be rejected after schema is fixed
        {"name": "Carol", "age": 28},
    ]))

    runner = CliRunner()
    result = runner.invoke(app, [
        "ingest", str(input_file),
        f"az://{CONTAINER}/delta/cli-test",
        "--dlq", f"az://{CONTAINER}/dlq/cli-test.ndjson",
        "--storage-option", f"AZURE_STORAGE_ACCOUNT_NAME={AZURITE_ACCOUNT}",
        "--storage-option", f"AZURE_STORAGE_ACCOUNT_KEY={AZURITE_KEY}",
        "--storage-option", "AZURE_STORAGE_USE_EMULATOR=true",
    ])

    assert result.exit_code == 0, result.output
    assert "written" in result.output
