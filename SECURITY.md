# Security Policy

## Supported versions

Security fixes target the current `main` branch unless a release branch is explicitly maintained.

## Reporting a vulnerability

Please do not open public issues for vulnerabilities. Report suspected security issues privately through GitHub Security Advisories or to the repository maintainers.

Include:

- affected version or commit
- reproduction steps
- affected command or generated output
- any sensitive-data exposure risk

## Handling migration secrets

SAS migration projects often involve database URLs, credentials, and production data paths. Do not commit `.env` files, generated reports containing credentials, or converted datasets with real sensitive data.