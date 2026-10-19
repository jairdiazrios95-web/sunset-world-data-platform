"""Unit-test GitHub run matching without network access."""
from scripts.verify_two_acceptance_runs import matching, EXPECTED_JOBS, WORKFLOW


def test_audit_workflow_selects_only_matching_sha_and_event():
    runs = [
        {"id": 11, "name": WORKFLOW, "event": "push", "head_sha": "sha-a"},
        {"id": 12, "name": WORKFLOW, "event": "workflow_dispatch", "head_sha": "sha-a"},
        {"id": 13, "name": WORKFLOW, "event": "push", "head_sha": "sha-b"},
        {"id": 14, "name": "unrelated", "event": "push", "head_sha": "sha-a"},
    ]
    assert matching(runs, "sha-a", "push", set())["id"] == 11
    assert matching(runs, "sha-a", "workflow_dispatch", {11})["id"] == 12
    assert matching(runs, "sha-a", "push", {11}) is None
    assert "acceptance" in EXPECTED_JOBS and len(EXPECTED_JOBS) == 6
