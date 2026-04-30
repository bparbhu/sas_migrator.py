# SAS PROC to Python Ecosystem Mapping

Not every SAS program should become pandas code. pandas is the table and
data-wrangling layer; modeling and analytics PROCs should target specialized
Python packages.

## Conversion Targets

| SAS area | Example PROCs | Python target | Notes |
| --- | --- | --- | --- |
| Data manipulation | SQL, SORT, TRANSPOSE | pandas, SQLAlchemy | Preserve SAS missing and merge semantics where pandas differs. |
| Descriptive statistics | FREQ, MEANS, SUMMARY, UNIVARIATE, CORR | pandas, scipy.stats | Use pandas for tables; scipy/statsmodels for tests and p-values. |
| Preprocessing / imputation | STDIZE | pandas, scikit-learn | `REPONLY` missing replacement can map to pandas fillna or sklearn SimpleImputer. |
| Numeric and array operations | DATA step functions, IML | numpy, scipy.linalg | Elementwise math, linear algebra, matrix decomposition, and vectorized operations. |
| Linear models / ANOVA | REG, GLM | statsmodels | Prefer statsmodels over sklearn when parameter estimates, p-values, ANOVA tables, contrasts, or confidence intervals matter. |
| Logistic / GLM models | LOGISTIC, GENMOD | statsmodels, scikit-learn | statsmodels for SAS-like inference; sklearn for predictive pipelines. |
| Mixed models | MIXED | statsmodels MixedLM | Covariance structure support must be validated option by option. |
| Survival analysis | PHREG, LIFETEST | lifelines, scikit-survival | Cox models and Kaplan-Meier workflows. |
| Time series | ARIMA, ESM, FORECAST | statsmodels | ARIMA/SARIMAX and exponential smoothing need explicit diagnostics and validation. |
| Machine learning | FASTCLUS, CLUSTER, DISCRIM, PRINCOMP, FACTOR | scikit-learn, factor_analyzer | Predictive modeling and decomposition. |
| Optimization | NLP, OPTMODEL | scipy.optimize, cvxpy | Requires dedicated expression/objective/constraint extraction. |
| Visualization | SGPLOT, GPLOT | matplotlib, seaborn, plotly | Output target determines static vs interactive choices. |
| Reporting | REPORT, TABULATE, PRINT | pandas, jinja2, openpyxl | Formatting/export is a reporting concern, not model logic. |

## Production Rule

The converter should classify each PROC before translating:

1. pandas-compatible transformation
2. statistical inference requiring statsmodels/scipy
3. predictive ML requiring scikit-learn
4. survival/time-series/optimization requiring domain packages
5. reporting/visualization
6. unsupported/manual review

Generated reports should show this classification so migration owners can staff
reviews correctly. A PROC GLM discrepancy is a statistics validation issue; a
PROC SQL join discrepancy is a data semantics issue; a PROC SGPLOT conversion is
a visualization target issue.

## Package Guidance

- Use `statsmodels` for SAS/STAT style inference because it exposes formula
  models, ANOVA tables, GLM families, regression summaries, and p-values.
- Use `scikit-learn` for predictive ML pipelines, cross-validation,
  preprocessing, clustering, classification, and model deployment patterns.
- Use `scipy.stats` for lower-level statistical tests and distributions.
- Use `numpy` for elementwise numeric functions, vectorized array expressions,
  missing-aware numeric reductions, random draws, and matrix-like operations.
- Use `scipy.linalg`, `scipy.optimize`, and `scipy.stats` where SAS code moves
  beyond table operations into linear algebra, optimization, distributions, and
  hypothesis tests.
- Use `lifelines` or `scikit-survival` for survival models.
- Use `statsmodels` time-series modules for ARIMA/SARIMAX and exponential
  smoothing families.
- Use pandas only when the SAS procedure is fundamentally table manipulation or
  descriptive aggregation.

## Sources

- SAS PROC GLM overview: https://support.sas.com/documentation/cdl/en/statug/63962/HTML/default/statug_glm_sect001.htm
- SAS PROC GLM syntax: https://support.sas.com/documentation/cdl/en/statug/63347/HTML/default/statug_glm_sect007.htm
- SAS PROC LOGISTIC overview: https://support.sas.com/documentation/cdl/en/statug/63347/HTML/default/statug_logistic_sect001.htm
- SAS PROC LOGISTIC syntax: https://support.sas.com/documentation/cdl/en/statug/63962/HTML/default/logistic_toc.htm
- SAS PROC REG statement: https://support.sas.com/documentation/cdl/en/statug/68162/HTML/default/statug_reg_syntax01.htm
- SAS PROC ARIMA overview: https://support.sas.com/documentation/cdl/en/etsug/68148/HTML/default/etsug_arima_overview.htm
- statsmodels formula API: https://www.statsmodels.org/v0.12.2/api.html
- statsmodels ANOVA: https://www.statsmodels.org/stable/anova.html
- scikit-learn LogisticRegression: https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html
- statsmodels SARIMAX examples: https://www.statsmodels.org/stable/examples/notebooks/generated/statespace_sarimax_stata.html
