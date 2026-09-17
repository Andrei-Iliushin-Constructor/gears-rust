"""HTTP workflow and explicit normalization for complete registration responses."""
import asyncio
from copy import deepcopy
from datetime import datetime
import json
import re
import uuid
from urllib.parse import urljoin

import httpx


GTS_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "gts")
RFC3339 = re.compile(
    r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})"
)


def assert_json(actual, expected):
    """Compare every field; JSON serialization also distinguishes true from 1."""
    assert json.dumps(actual, indent=2, sort_keys=True) == json.dumps(
        expected, indent=2, sort_keys=True
    )


def timestamp(value):
    assert isinstance(value, str) and RFC3339.fullmatch(value), value
    return datetime.fromisoformat(value.replace("Z", "+00:00").replace("z", "+00:00"))


def assert_uuid(value):
    assert isinstance(value, str), value
    parsed = uuid.UUID(value)
    assert str(parsed) == value and parsed.int != 0, value


def replace_text(document, field):
    """Only explicitly named, non-contractual text may be normalized."""
    assert isinstance(document[field], str) and document[field].strip(), document
    document[field] = f"<{field}>"


def assert_operation(operation, expected):
    actual = deepcopy(operation)
    assert_uuid(actual["operation_id"])
    assert timestamp(actual["created_at"]) <= timestamp(actual["started_at"]) <= timestamp(
        actual["completed_at"]
    ), actual
    for field in ("operation_id", "created_at", "started_at", "completed_at"):
        actual[field] = f"<{field}>"
    for item in actual["items"]:
        if item["error"] is not None:
            replace_text(item["error"], "message")
    actual["items"].sort(key=lambda item: item["gts_id"])
    expected = deepcopy(expected)
    expected["items"].sort(key=lambda item: item["gts_id"])
    assert_json(actual, expected)


async def read_created(client, api_path, expected, operation):
    response = await client.get(f"{api_path}/entities/{expected['gts_id']}")
    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("application/json")
    entity = response.json()
    actual = deepcopy(entity)
    # These fixtures use named GTS IDs (no UUID tail). The Registry Reference
    # must be UUIDv5(URL-namespace UUIDv5("gts"), full GTS ID), not any valid UUID.
    assert actual["gts_uuid"] == str(uuid.uuid5(GTS_NAMESPACE, expected["gts_id"]))
    created = timestamp(actual["created_at"])
    assert created == timestamp(actual["updated_at"]), actual
    assert timestamp(operation["started_at"]) <= created <= timestamp(
        operation["completed_at"]
    ), actual
    for field in ("gts_uuid", "created_at", "updated_at"):
        actual[field] = f"<{field}>"
    assert_json(actual, expected)
    return entity


def assert_not_found(response, expected):
    assert response.status_code == 404, response.text
    assert response.headers["content-type"].startswith("application/problem+json")
    actual = response.json()
    assert actual["instance"] == response.request.url.path, actual
    actual["instance"] = "<request_path>"
    replace_text(actual, "trace_id")
    replace_text(actual, "detail")
    assert_json(actual, expected)


async def submit_and_poll(client, api_path, candidates, expected_receipt):
    """Return terminal outcomes; completed never implies all items succeeded."""
    response = await client.post(
        f"{api_path}/entities",
        headers={"Idempotency-Key": str(uuid.uuid4())},
        json={"items": candidates},
    )
    assert response.status_code == 202, response.text
    assert response.headers["content-type"].startswith("application/json")
    receipt = response.json()
    assert_uuid(receipt["operation_id"])
    assert receipt["status"] in {"pending", "running", "completed"}, receipt
    normalized = {**receipt, "operation_id": "<operation_id>", "status": "<status>"}
    assert_json(normalized, expected_receipt)
    assert "location" in response.headers, response.headers
    # Follow the actual Location, including any gateway prefix.
    location = urljoin(str(response.url), response.headers["location"])
    last_operation = None
    # A latency requirement, not a tuning knob: an accepted registration must
    # reach a terminal status promptly. Exhausting this deadline means the work
    # waited for some periodic sweep instead of being picked up on acceptance.
    try:
        async with asyncio.timeout(4):
            while True:
                polled = await client.get(location)
                assert polled.status_code == 200, polled.text
                assert polled.headers["content-type"].startswith("application/json")
                last_operation = polled.json()
                assert last_operation["operation_id"] == receipt["operation_id"]
                assert last_operation["kind"] == "registration", last_operation
                assert last_operation["dry_run"] is False, last_operation
                status = last_operation["status"]
                assert status in {"pending", "running", "completed"}, last_operation
                if status == "completed":
                    return last_operation
                await asyncio.sleep(0.05)
    except (TimeoutError, httpx.TimeoutException):
        raise AssertionError(
            f"Operation {receipt['operation_id']} did not complete at {location}; "
            f"last operation (including item errors): {last_operation}"
        ) from None
