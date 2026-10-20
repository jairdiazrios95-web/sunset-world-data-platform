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
