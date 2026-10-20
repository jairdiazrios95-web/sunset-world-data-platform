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

## Explicitly reviewed S3 compatibility exception (S7608)

Sonar rule `python:S7608` requires AWS `ExpectedBucketOwner` (an AWS account
ID) on S3 calls. This deployment uses **MinIO**, not AWS S3, and has no AWS
owner-account ID. Passing an invented ID would either break MinIO or create
misleading security. We narrowly suppress S7608 for `sunset/minio.py` and
`scripts/verify_minio_live.py` only; the original findings and justification
remain in this document. The exclusion is **not** a claim of production-grade
bucket authorization or identity-based access control. MinIO IAM policies,
least-privilege credentials and TLS must be assessed with the actual host.

The Oracle extractor now uses immutable, literal SQL templates selected by
a strictly allowlisted table key; **no SQL identifier is interpolated into a
query**. The watermark remains a bind variable. The general 100-table Oracle
extension still requires an explicit source-query mapping and vendor access.
The streaming image builds as root only to install dependencies and defaults
to unprivileged UID 185 after installation; verify Spark checkpoint mounts in
CI before any production use.
