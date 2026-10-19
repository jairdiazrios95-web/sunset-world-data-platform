"""Assert two independent real GitHub Actions acceptance executions at one commit.

The first is the automatic push-triggered run. The second is a new
workflow_dispatch run (new run ID), not a rerun of stale artifacts.
Requires GITHUB_TOKEN with actions:write on the orchestration job.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import time


EXPECTED_JOBS = {
    "platform / live-postgres-parquet",
    "minio / minio",
    "airflow / live-airflow",
    "streaming / live-streaming",
    "rag / real-rag",
    "acceptance",
}
WORKFLOW = "sunset-real-stack-acceptance"


def api(route: str, method: str = "GET", fields: tuple[str, ...] = ()):
    command = ["gh", "api", "-X", method, route]
    for field in fields:
        command.extend(["-f", field])
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        # Prevent accidental printing of tokens, env or HTTP Authorization
        raise RuntimeError("GitHub API request failed for " + route
                           + ": " + result.stderr[-500:])
    if not result.stdout.strip():
        return None
    return json.loads(result.stdout)


def matching(runs: list[dict], sha: str, event: str, excluded: set[int]):
    candidates = [
        run for run in runs
        if run.get("head_sha") == sha and run.get("name") == WORKFLOW
        and run.get("event") == event and run.get("id") not in excluded
    ]
    return min(candidates, key=lambda x: x["id"]) if candidates else None


def list_runs(repo: str, sha: str):
    response = api(f"repos/{repo}/actions/runs?head_sha={sha}&per_page=100")
    return response["workflow_runs"]


def poll_run(repo: str, sha: str, event: str, excluded: set[int],
             deadline: float):
    while time.monotonic() < deadline:
        candidate = matching(list_runs(repo, sha), sha, event, excluded)
        if candidate:
            return candidate["id"]
        time.sleep(15)
    raise TimeoutError("Did not find independent " + event + " acceptance run")


def validate_run(repo: str, run_id: int, sha: str, deadline: float):
    while time.monotonic() < deadline:
        run = api(f"repos/{repo}/actions/runs/{run_id}")
        if run["head_sha"] != sha:
            raise AssertionError("Unexpected SHA for acceptance run")
        if run["status"] != "completed":
            time.sleep(20)
            continue
        if run["conclusion"] != "success":
            raise AssertionError(
                f"Acceptance run {run_id} finished: {run['conclusion']}"
            )
        data = api(f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100")
        jobs = data["jobs"]
        successful = {j["name"] for j in jobs if j.get("conclusion") == "success"}
        missing = EXPECTED_JOBS - successful
        if missing:
            raise AssertionError(f"Run {run_id} missing successful jobs: {sorted(missing)}")
        artifacts = api(f"repos/{repo}/actions/runs/{run_id}/artifacts?per_page=100")
        names = {a["name"] for a in artifacts.get("artifacts", [])}
        if "sunset-real-acceptance-summary" not in names:
            raise AssertionError(f"Run {run_id} lacks acceptance summary artifact")
        return {
            "run_id": run_id,
            "sha": sha,
            "url": run["html_url"],
            "conclusion": run["conclusion"],
            "jobs_verified": sorted(successful & EXPECTED_JOBS),
            "acceptance_artifact": "sunset-real-acceptance-summary",
        }
    raise TimeoutError(f"Acceptance run {run_id} exceeded deadline")


def main():
    repo = os.environ["GITHUB_REPOSITORY"]
    sha = os.environ["GITHUB_SHA"]
    output = Path("reports/audit_two_passes.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + 45 * 60
    first_id = poll_run(repo, sha, "push", set(), deadline)
    first = validate_run(repo, first_id, sha, deadline)
    print("PASS 1:", first["url"], flush=True)
    before = {r["id"] for r in list_runs(repo, sha)}
    api(f"repos/{repo}/actions/workflows/real-acceptance.yml/dispatches",
        method="POST", fields=("ref=main",))
    second_id = poll_run(repo, sha, "workflow_dispatch", before, deadline)
    if second_id == first_id:
        raise AssertionError("Second acceptance must be a new workflow run")
    second = validate_run(repo, second_id, sha, deadline)
    print("PASS 2:", second["url"], flush=True)
    evidence = {"status": "PASS", "sha": sha, "independent_runs": [first, second],
                "same_commit": True, "real_stack_jobs_per_pass": len(EXPECTED_JOBS),
                "production_approved": False}
    output.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print("TWO INDEPENDENT FULL ACCEPTANCE RUNS PASSED", flush=True)


if __name__ == "__main__":
    main()
