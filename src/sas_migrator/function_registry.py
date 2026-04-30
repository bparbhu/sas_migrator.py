from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class FunctionMapping:
    sas_name: str
    python_target: str
    package: str
    category: str
    notes: str


FUNCTION_MAPPINGS: dict[str, FunctionMapping] = {
    "abs": FunctionMapping("abs", "np.abs", "numpy", "numeric", "Elementwise absolute value."),
    "sqrt": FunctionMapping("sqrt", "np.sqrt", "numpy", "numeric", "Elementwise square root."),
    "log": FunctionMapping("log", "np.log", "numpy", "numeric", "Natural logarithm."),
    "log10": FunctionMapping("log10", "np.log10", "numpy", "numeric", "Base-10 logarithm."),
    "exp": FunctionMapping("exp", "np.exp", "numpy", "numeric", "Elementwise exponential."),
    "round": FunctionMapping("round", "np.round", "numpy", "numeric", "Rounding semantics need validation for SAS parity."),
    "ceil": FunctionMapping("ceil", "np.ceil", "numpy", "numeric", "Ceiling."),
    "floor": FunctionMapping("floor", "np.floor", "numpy", "numeric", "Floor."),
    "sum": FunctionMapping("sum", "np.nansum", "numpy", "aggregation", "SAS sum ignores missing values."),
    "mean": FunctionMapping("mean", "np.nanmean", "numpy", "aggregation", "SAS mean ignores missing values."),
    "median": FunctionMapping("median", "np.nanmedian", "numpy", "aggregation", "Median ignoring missing values."),
    "min": FunctionMapping("min", "np.nanmin", "numpy", "aggregation", "Minimum ignoring missing values."),
    "max": FunctionMapping("max", "np.nanmax", "numpy", "aggregation", "Maximum ignoring missing values."),
    "std": FunctionMapping("std", "np.nanstd", "numpy", "aggregation", "Standard deviation; ddof must match SAS option."),
    "var": FunctionMapping("var", "np.nanvar", "numpy", "aggregation", "Variance; ddof must match SAS option."),
    "lag": FunctionMapping("lag", "Series.shift", "pandas", "row_state", "SAS LAG queue semantics can differ from naive shift in conditional code."),
    "dif": FunctionMapping("dif", "Series.diff", "pandas", "row_state", "Difference with prior value; conditional queue semantics need review."),
    "int": FunctionMapping("int", "np.trunc", "numpy", "numeric", "SAS INT truncates toward zero."),
    "mod": FunctionMapping("mod", "np.mod", "numpy", "numeric", "Modulo."),
    "ranuni": FunctionMapping("ranuni", "np.random.default_rng(seed).uniform", "numpy", "random", "Random stream reproducibility must be configured."),
    "rannor": FunctionMapping("rannor", "np.random.default_rng(seed).normal", "numpy", "random", "Random stream reproducibility must be configured."),
    "probnorm": FunctionMapping("probnorm", "scipy.stats.norm.cdf", "scipy", "distribution", "Normal CDF."),
    "quantile": FunctionMapping("quantile", "scipy.stats distribution ppf", "scipy", "distribution", "Distribution-specific inverse CDF."),
    "cdf": FunctionMapping("cdf", "scipy.stats distribution cdf", "scipy", "distribution", "Distribution-specific CDF."),
    "pdf": FunctionMapping("pdf", "scipy.stats distribution pdf", "scipy", "distribution", "Distribution-specific PDF."),
}


def classify_function(name: str) -> FunctionMapping | None:
    return FUNCTION_MAPPINGS.get(name.lower())


def function_mapping_table() -> list[dict]:
    return [asdict(mapping) for mapping in sorted(FUNCTION_MAPPINGS.values(), key=lambda item: item.sas_name)]
