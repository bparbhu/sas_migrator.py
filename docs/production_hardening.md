# Production Hardening Backlog

This project is intentionally deterministic: parsing, IR generation, translation, and validation should not depend on an LLM at runtime. The items below are the next highest-value additions for production migration programs.

## Translation coverage

- Expand PROC SQL support for correlated subqueries, unions, calculated columns, HAVING, and SAS-specific date functions.
- Add DATA step array, DO/OUTPUT loop, hash object, lag queue, and format/informat handling.
- Add explicit semantic warnings for unsupported implicit SAS behavior, especially type coercion and missing-value ordering.
- Grow PROC routing for modeling workloads into scikit-learn, statsmodels, scipy, lifelines, and Spark ML.

## Validation

- Add golden-output fixtures for joins with duplicate keys, missing values, BY-group ordering, and date/time edge cases.
- Add optional Spark runtime equivalence tests behind a marker or environment flag.
- Add fixture generators for large synthetic SAS-like datasets so performance regressions are measurable.

## Operations

- Publish Docker images from tagged releases after CI passes.
- Add SBOM and dependency vulnerability scanning for release artifacts.
- Add structured JSON logging for batch conversion runs.
- Add a `coverage-report` command that summarizes supported, partial, and unsupported constructs across a SAS tree.

## Governance

- Require PRs to include conversion examples or fixtures for new syntax support.
- Keep unsupported constructs explicit in reports rather than silently producing approximate code.
- Track migration readiness separately from translation success so teams can plan data access, validation, and deployment work.