"""Shared JSON scenario data and HTTP fixtures for async registration."""

import json
import os
from pathlib import Path
import re
import uuid

import httpx
import pytest


SCENARIO_TESTS = pytest.StashKey[dict[str, list[str]]]()


@pytest.fixture
def registry_api_path():
    """T24a changes this default to v1; there is intentionally no fallback."""
    return f"types-registry/{os.getenv('TYPES_REGISTRY_API_VERSION', 'v2')}"


@pytest.fixture
async def registry_http(base_url, auth_headers):
    async with httpx.AsyncClient(
        base_url=base_url.rstrip("/") + "/", headers=auth_headers, timeout=3.0
    ) as client:
        yield client


@pytest.fixture
def registration_fixture():
    """Load the files linked from registration.md with one namespace per test."""
    namespace = f"r{uuid.uuid4().hex}"
    directory = Path(__file__).parent / "fixtures" / "registration"

    def load(name):
        document = (directory / f"{name}.json").read_text(encoding="utf-8")
        return json.loads(
            document.replace("cf.e2e.registration.", f"cf.e2e.{namespace}.")
        )

    return load


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "scenario(id): stable scenario ID from scenarios/registration.md"
    )


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config, items):
    """Capture associations before -k/-m deselection; permit planned scenarios."""
    document = Path(__file__).parent / "scenarios" / "registration.md"
    ids = re.findall(
        r"^### (TR-REG-\d{3}) —", document.read_text(encoding="utf-8"), re.M
    )
    if len(ids) != len(set(ids)):
        raise pytest.UsageError(f"Duplicate scenario IDs in {document}")
    associations = {scenario_id: [] for scenario_id in ids}
    for item in items:
        if Path(__file__).parent not in item.path.parents:
            continue
        for marker in item.iter_markers("scenario"):
            if len(marker.args) != 1 or marker.args[0] not in associations:
                raise pytest.UsageError(
                    f"Unknown registration scenario on {item.nodeid}: {marker.args}"
                )
            scenario_id = marker.args[0]
            associations[scenario_id].append(item.nodeid)
            item.user_properties.append(("scenario", scenario_id))
    config.stash[SCENARIO_TESTS] = associations


def pytest_terminal_summary(terminalreporter):
    """Keep automation links separate from actual results, including deselection."""
    associations = terminalreporter.config.stash.get(SCENARIO_TESTS, {})
    if not associations:
        return
    terminalreporter.section("Types Registry registration scenarios")
    reports = [
        report
        for group in terminalreporter.stats.values()
        for report in group
        if isinstance(report, pytest.TestReport)
    ]
    for scenario_id, nodeids in associations.items():
        if not nodeids:
            terminalreporter.write_line(f"{scenario_id}: no test collected")
        for nodeid in nodeids:
            results = [report for report in reports if report.nodeid == nodeid]
            if any(report.failed for report in results):
                outcome = "failed"
            elif any(report.skipped for report in results):
                outcome = "skipped"
            elif any(report.when == "call" and report.passed for report in results):
                outcome = "passed"
            else:
                outcome = "not run"
            terminalreporter.write_line(f"{scenario_id}: {outcome} — {nodeid}")
