## Real SonarQube Community without external credentials
The workflow `ephemeral-sonarqube.yml` starts an isolated real Community
server + PostgreSQL using GitHub Actions runners, creates a temporary
project/token through its REST API, runs the official scanner, waits for the
**actual server Quality Gate** and uploads measured evidence. The server and
token are ephemeral and are discarded after the run. **This is a real Sonar
analysis, but is not a permanent SonarQube dashboard.**

The workflow is resource-intensive; hosted runners may hit image limits or
elasticsearch vm.max_map_count requirements. When it fails, it must remain
FAILED until a real gate executes. It does not substitute for a company
SonarCloud/SonarQube project with persistent history, which still needs a
managed URL + token.

The 90% coverage target applies to the **same repository-wide Python
statement denominator** (all modules in the original `--cov` arguments).
No modules are excluded to inflate results.
