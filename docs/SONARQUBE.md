# SonarQube / static analysis integration

A GitHub Actions job `sunset-static-security-and-sonarqube` performs
**actual** local static syntax/undefined-name validation (Ruff), high-confidence
high-severity Bandit checks, and measured pytest coverage. Reports are uploaded
as immutable CI artifacts. These checks run without outside accounts.

The same workflow has a **separate actual SonarQube scan job**. It is **skipped**
until the repository has:

1. `Settings > Secrets and variables > Actions > Variables`:
   `SONAR_HOST_URL` (an accessible SonarQube URL or SonarCloud-compatible host).
2. `Settings > Secrets and variables > Actions > Secrets`:
   `SONAR_TOKEN` (server-issued analysis token, never commit the value).
3. Project key matching `sonar.projectKey` in `sonar-project.properties`,
   plus network reachability from GitHub-hosted runners; for a private
   company-only server a self-hosted runner may be necessary.

The scanner and server Quality Gate are not simulated: the workflow requires
a successful scan and Quality Gate result. Without these details the SonarQube
job must remain skipped, and **no SonarQube PASS may be claimed**. The
standalone Python checks are not a replacement for the SonarQube dashboard.

Security/privacy: do not set the repository public or paste server tokens in
chat, source code, Docker images, artifacts, or logs.

Technical caveat: measured coverage from offline test modules is not complete
coverage of Docker/Oracle/Mantis integrations; use actual service integration
runs as separate evidence.

## Verified ephemeral SonarQube Community quality analysis

There is **also** an independent, real self-hosted-for-CI-only SonarQube Community
workflow (`.github/workflows/ephemeral-sonarqube.yml`). It runs SonarQube and
PostgreSQL in disposable Docker containers, does not require an externally
managed `SONAR_HOST_URL` or `SONAR_TOKEN`, and asserts results from the
actual SonarQube REST API. This is distinct from the optional persistent-server
job above; that optional job remains skipped without external credentials.

The [successful real SonarQube run #39353520037](https://github.com/jairdiazrios95-web/sunset-world-data-platform/actions/runs/39353520037)
(at SHA `6178e11`) reported **91.1% overall Python coverage, zero bugs, zero
vulnerabilities, zero code smells and Quality Gate OK**. The quality workflow
now independently blocks Python coverage below 90%; ephemeral Sonar validates
metric presence and all four values and runs on pull requests as well as pushes.

A transient server does not preserve longitudinal dashboards. Check that the
Quality Gate has passed **on the exact commit proposed for merging**, not just
an earlier commit, and separately confirm end-to-end acceptance. No claim of
production approval is made.
