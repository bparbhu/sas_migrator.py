# Migration Program Readiness

SAS-to-Python migration is not just syntax conversion. A production program also
needs operating-model readiness.

## Required Workstreams

1. Standardized environment
   - Conda and pip setup files
   - pinned core dependencies
   - optional modeling stack for statsmodels, scikit-learn, NumPy, SciPy, and survival analysis

2. Performance planning
   - classify jobs by data volume and operation type
   - prefer SQL pushdown for database-heavy code
   - benchmark pandas against alternatives when needed

3. Validation framework
   - generated Python syntax validation
   - row count and schema checks
   - aggregate and checksum checks
   - statistical tolerance comparisons
   - edge-case fixtures for missing values, BY groups, joins, and dates

4. Workflow integration
   - scheduler integration
   - logging and observability
   - generated documentation and ownership handoff
   - upstream/downstream contract review

5. AI-assisted review
   - optional LLM use for explanations, documentation, test generation, and review acceleration
   - deterministic parser/IR/translator remains the source of truth
   - all AI-generated changes require validation artifacts

## Tool Support

The converter writes `migration_readiness.json` on each `translate-tree` run and
also exposes:

```bash
sas-migrator readiness <source_root> --output migration_readiness.json
```

## Article Driver

This readiness layer was added after reviewing:

- https://medium.com/@annadu01/from-sas-to-python-navigating-the-ai-powered-migration-journey-9e71a325dc2c
