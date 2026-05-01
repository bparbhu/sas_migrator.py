from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ProcMapping:
    proc: str
    category: str
    primary_package: str
    fallback_package: str | None
    distributed_package: str | None
    confidence: str
    notes: str


PROC_MAPPINGS: dict[str, ProcMapping] = {
    "sql": ProcMapping("sql", "data_manipulation", "pandas", "SQLAlchemy", "pyspark", "high", "Table queries, joins, filters, and aggregations."),
    "sort": ProcMapping("sort", "data_manipulation", "pandas", "pyspark", None, "high", "Sorting and BY preparation; Spark is a candidate for distributed data."),
    "transpose": ProcMapping("transpose", "data_reshaping", "pandas", None, "pyspark", "high", "Pivot and melt style reshaping."),
    "freq": ProcMapping("freq", "descriptive_statistics", "pandas", "scipy.stats", "pyspark", "high", "Frequency tables; tests need scipy/statsmodels."),
    "means": ProcMapping("means", "descriptive_statistics", "pandas", "scipy.stats", "pyspark", "high", "Group summaries and descriptive statistics."),
    "summary": ProcMapping("summary", "descriptive_statistics", "pandas", "scipy.stats", "pyspark", "high", "Group summaries and descriptive statistics."),
    "contents": ProcMapping("contents", "metadata", "pandas", "pyspark", None, "high", "Dataset metadata, schema, and column inspection."),
    "append": ProcMapping("append", "data_manipulation", "pandas", "pyspark", None, "high", "Append/union datasets; Spark unionByName handles distributed schemas."),
    "rank": ProcMapping("rank", "data_manipulation", "pandas", "pyspark", None, "medium", "Ranking and quantile groups; Spark window functions or ntile for distributed data."),
    "stdize": ProcMapping("stdize", "preprocessing", "pandas", "scikit-learn", "Spark ML", "medium", "Standardization and missing-value replacement; Spark ML Imputer/StandardScaler are distributed candidates."),
    "univariate": ProcMapping("univariate", "descriptive_statistics", "scipy.stats", "pandas", None, "medium", "Distribution summaries, quantiles, normality tests."),
    "corr": ProcMapping("corr", "descriptive_statistics", "numpy", "scipy.stats", "pyspark", "high", "Correlation matrices and correlation tests; Spark can compute large distributed correlations."),
    "ttest": ProcMapping("ttest", "statistical_tests", "scipy.stats", None, None, "high", "t-tests and confidence intervals."),
    "npar1way": ProcMapping("npar1way", "statistical_tests", "scipy.stats", None, None, "medium", "Nonparametric tests; exact options need review."),
    "anova": ProcMapping("anova", "statistical_tests", "statsmodels", "scipy.stats", None, "medium", "ANOVA tables and model-based tests."),
    "reg": ProcMapping("reg", "statistical_modeling", "statsmodels", "scikit-learn", "Spark ML", "high", "OLS regression; Spark ML LinearRegression is a distributed predictive alternative."),
    "glm": ProcMapping("glm", "statistical_modeling", "statsmodels", None, "Spark ML", "medium", "General linear models; Spark ML GeneralizedLinearRegression is a distributed candidate when inference parity is not required."),
    "mixed": ProcMapping("mixed", "statistical_modeling", "statsmodels", None, None, "medium", "Mixed effects models; covariance structures often need manual review."),
    "logistic": ProcMapping("logistic", "statistical_modeling", "statsmodels", "scikit-learn", "Spark ML", "medium", "Use statsmodels for inference, sklearn for local predictive pipelines, and Spark ML LogisticRegression for distributed predictive modeling."),
    "genmod": ProcMapping("genmod", "statistical_modeling", "statsmodels", None, "Spark ML", "medium", "Generalized linear models; Spark ML GeneralizedLinearRegression can cover distributed predictive cases."),
    "phreg": ProcMapping("phreg", "survival_analysis", "lifelines", "scikit-survival", None, "medium", "Cox proportional hazards and survival models."),
    "lifetest": ProcMapping("lifetest", "survival_analysis", "lifelines", None, None, "medium", "Kaplan-Meier and survival tests."),
    "arima": ProcMapping("arima", "time_series", "statsmodels", None, None, "medium", "ARIMA/SARIMAX; identification and diagnostics need validation."),
    "esm": ProcMapping("esm", "time_series", "statsmodels", None, None, "low", "Exponential smoothing; SAS options need careful mapping."),
    "forecast": ProcMapping("forecast", "time_series", "statsmodels", "prophet", None, "low", "Forecasting workflows vary by SAS options."),
    "cluster": ProcMapping("cluster", "machine_learning", "scikit-learn", None, "Spark ML", "medium", "Clustering; Spark ML clustering is a distributed candidate."),
    "fastclus": ProcMapping("fastclus", "machine_learning", "scikit-learn", None, "Spark ML", "medium", "K-means style clustering; Spark ML KMeans is a distributed candidate."),
    "discrim": ProcMapping("discrim", "machine_learning", "scikit-learn", None, "Spark ML", "medium", "LDA/QDA classifiers; Spark ML classifiers may fit predictive substitutes depending on options."),
    "princomp": ProcMapping("princomp", "machine_learning", "scikit-learn", None, "Spark ML", "high", "PCA; Spark ML PCA is a distributed candidate."),
    "iml": ProcMapping("iml", "matrix_programming", "numpy", "scipy.linalg", None, "low", "Interactive matrix language maps to NumPy/SciPy but needs expression-level parsing."),
    "factor": ProcMapping("factor", "machine_learning", "factor_analyzer", "sklearn.decomposition", "Spark ML", "medium", "Factor analysis; rotation options matter, Spark ML PCA can be a distributed dimensionality fallback."),
    "nlp": ProcMapping("nlp", "optimization", "scipy.optimize", "cvxpy", None, "low", "Nonlinear optimization needs manual objective/constraint extraction."),
    "optmodel": ProcMapping("optmodel", "optimization", "scipy.optimize", "cvxpy", None, "low", "Optimization model language requires dedicated parser."),
    "sgplot": ProcMapping("sgplot", "visualization", "matplotlib", "seaborn", None, "medium", "Static plots."),
    "gplot": ProcMapping("gplot", "visualization", "matplotlib", "seaborn", None, "medium", "Legacy plotting."),
    "report": ProcMapping("report", "reporting", "pandas", "jinja2", None, "medium", "Tabular reports; styling/export depends on target."),
    "tabulate": ProcMapping("tabulate", "reporting", "pandas", None, None, "medium", "Cross-tabulated reports."),
    "print": ProcMapping("print", "reporting", "pandas", None, None, "high", "Display/export table rows."),
    "export": ProcMapping("export", "io", "pandas", None, "pyspark", "high", "CSV/Excel/database export depending on DBMS."),
    "import": ProcMapping("import", "io", "pandas", "pyreadstat", "pyspark", "high", "CSV/Excel/SAS dataset import depending on source."),
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
            distributed_package=None,
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