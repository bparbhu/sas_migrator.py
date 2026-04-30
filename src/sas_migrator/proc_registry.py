from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ProcMapping:
    proc: str
    category: str
    primary_package: str
    fallback_package: str | None
    confidence: str
    notes: str


PROC_MAPPINGS: dict[str, ProcMapping] = {
    "sql": ProcMapping("sql", "data_manipulation", "pandas", "SQLAlchemy", "high", "Table queries, joins, filters, and aggregations."),
    "sort": ProcMapping("sort", "data_manipulation", "pandas", "pyspark", "high", "Sorting and BY preparation; Spark is a candidate for distributed data."),
    "transpose": ProcMapping("transpose", "data_reshaping", "pandas", None, "high", "Pivot and melt style reshaping."),
    "freq": ProcMapping("freq", "descriptive_statistics", "pandas", "scipy.stats", "high", "Frequency tables; tests need scipy/statsmodels."),
    "means": ProcMapping("means", "descriptive_statistics", "pandas", "scipy.stats", "high", "Group summaries and descriptive statistics."),
    "summary": ProcMapping("summary", "descriptive_statistics", "pandas", "scipy.stats", "high", "Group summaries and descriptive statistics."),
    "contents": ProcMapping("contents", "metadata", "pandas", "pyspark", "high", "Dataset metadata, schema, and column inspection."),
    "append": ProcMapping("append", "data_manipulation", "pandas", "pyspark", "high", "Append/union datasets; Spark unionByName handles distributed schemas."),
    "rank": ProcMapping("rank", "data_manipulation", "pandas", "pyspark", "medium", "Ranking and quantile groups; Spark window functions or ntile for distributed data."),
    "stdize": ProcMapping("stdize", "preprocessing", "pandas", "scikit-learn", "medium", "Standardization and missing-value replacement; REponly imputation can map to fillna/SimpleImputer."),
    "univariate": ProcMapping("univariate", "descriptive_statistics", "scipy.stats", "pandas", "medium", "Distribution summaries, quantiles, normality tests."),
    "corr": ProcMapping("corr", "descriptive_statistics", "numpy", "scipy.stats", "high", "Correlation matrices and correlation tests."),
    "ttest": ProcMapping("ttest", "statistical_tests", "scipy.stats", None, "high", "t-tests and confidence intervals."),
    "npar1way": ProcMapping("npar1way", "statistical_tests", "scipy.stats", None, "medium", "Nonparametric tests; exact options need review."),
    "anova": ProcMapping("anova", "statistical_tests", "statsmodels", "scipy.stats", "medium", "ANOVA tables and model-based tests."),
    "reg": ProcMapping("reg", "statistical_modeling", "statsmodels", "scikit-learn", "high", "OLS regression; use statsmodels when SAS-like inference tables matter."),
    "glm": ProcMapping("glm", "statistical_modeling", "statsmodels", None, "medium", "General linear models, ANOVA/ANCOVA; sum-of-squares type and contrasts require validation."),
    "mixed": ProcMapping("mixed", "statistical_modeling", "statsmodels", None, "medium", "Mixed effects models; covariance structures often need manual review."),
    "logistic": ProcMapping("logistic", "statistical_modeling", "statsmodels", "scikit-learn", "medium", "Use statsmodels for inference; sklearn for predictive pipelines."),
    "genmod": ProcMapping("genmod", "statistical_modeling", "statsmodels", None, "medium", "Generalized linear models; link/distribution must be explicit."),
    "phreg": ProcMapping("phreg", "survival_analysis", "lifelines", "scikit-survival", "medium", "Cox proportional hazards and survival models."),
    "lifetest": ProcMapping("lifetest", "survival_analysis", "lifelines", None, "medium", "Kaplan-Meier and survival tests."),
    "arima": ProcMapping("arima", "time_series", "statsmodels", None, "medium", "ARIMA/SARIMAX; identification and diagnostics need validation."),
    "esm": ProcMapping("esm", "time_series", "statsmodels", None, "low", "Exponential smoothing; SAS options need careful mapping."),
    "forecast": ProcMapping("forecast", "time_series", "statsmodels", "prophet", "low", "Forecasting workflows vary by SAS options."),
    "cluster": ProcMapping("cluster", "machine_learning", "scikit-learn", None, "medium", "Clustering; distance/linkage/options require mapping."),
    "fastclus": ProcMapping("fastclus", "machine_learning", "scikit-learn", None, "medium", "K-means style clustering."),
    "discrim": ProcMapping("discrim", "machine_learning", "scikit-learn", None, "medium", "LDA/QDA classifiers."),
    "princomp": ProcMapping("princomp", "machine_learning", "scikit-learn", None, "high", "PCA."),
    "iml": ProcMapping("iml", "matrix_programming", "numpy", "scipy.linalg", "low", "Interactive matrix language maps to NumPy/SciPy but needs expression-level parsing."),
    "factor": ProcMapping("factor", "machine_learning", "factor_analyzer", "sklearn.decomposition", "medium", "Factor analysis; rotation options matter."),
    "nlp": ProcMapping("nlp", "optimization", "scipy.optimize", "cvxpy", "low", "Nonlinear optimization needs manual objective/constraint extraction."),
    "optmodel": ProcMapping("optmodel", "optimization", "scipy.optimize", "cvxpy", "low", "Optimization model language requires dedicated parser."),
    "sgplot": ProcMapping("sgplot", "visualization", "matplotlib", "seaborn", "medium", "Static plots."),
    "gplot": ProcMapping("gplot", "visualization", "matplotlib", "seaborn", "medium", "Legacy plotting."),
    "report": ProcMapping("report", "reporting", "pandas", "jinja2", "medium", "Tabular reports; styling/export depends on target."),
    "tabulate": ProcMapping("tabulate", "reporting", "pandas", None, "medium", "Cross-tabulated reports."),
    "print": ProcMapping("print", "reporting", "pandas", None, "high", "Display/export table rows."),
    "export": ProcMapping("export", "io", "pandas", None, "high", "CSV/Excel/database export depending on DBMS."),
    "import": ProcMapping("import", "io", "pandas", "pyreadstat", "high", "CSV/Excel/SAS dataset import depending on source."),
}


def classify_proc(proc_name: str) -> ProcMapping:
    key = proc_name.lower()
    return PROC_MAPPINGS.get(
        key,
        ProcMapping(
            proc=key,
            category="unknown",
            primary_package="manual_review",
            fallback_package=None,
            confidence="low",
            notes="No mapping registered yet; keep as unsupported until triaged.",
        ),
    )


def build_ecosystem_plan(manifest: dict) -> dict:
    proc_counts: dict[str, int] = {}
    for item in manifest.get("files", []):
        for proc in item.get("procs_used", []):
            proc_counts[proc] = proc_counts.get(proc, 0) + 1
    mappings = []
    for proc, count in sorted(proc_counts.items()):
        mapping = classify_proc(proc)
        row = asdict(mapping)
        row["count"] = count
        mappings.append(row)
    return {"proc_count": sum(proc_counts.values()), "unique_proc_count": len(proc_counts), "mappings": mappings}
