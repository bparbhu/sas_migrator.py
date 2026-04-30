# SAS Function to NumPy/SciPy Mapping

Many SAS expressions are not pandas operations. They are scalar, vectorized,
distribution, random-number, matrix, or statistical-test operations. These
should map to NumPy and SciPy where possible.

## Common Function Targets

| SAS function area | Examples | Python target |
| --- | --- | --- |
| Elementwise numeric math | `ABS`, `SQRT`, `LOG`, `LOG10`, `EXP`, `ROUND`, `CEIL`, `FLOOR` | `numpy` |
| Missing-aware reductions | `SUM`, `MEAN`, `MEDIAN`, `MIN`, `MAX`, `STD`, `VAR` | `numpy.nan*` functions |
| Row-state functions | `LAG`, `DIF` | pandas shift/diff plus SAS queue validation |
| Random draws | `RANUNI`, `RANNOR` | `numpy.random.Generator` |
| Distribution functions | `PROBNORM`, `CDF`, `PDF`, `QUANTILE` | `scipy.stats` |
| Linear algebra / matrices | PROC IML operations | `numpy`, `scipy.linalg` |
| Optimization | PROC NLP / OPTMODEL functions | `scipy.optimize`, `cvxpy` |

## Production Notes

- SAS `SUM`, `MEAN`, and related functions ignore missing values; naive Python
  operators usually propagate `NaN`. Prefer `np.nansum`, `np.nanmean`, etc.
- SAS `LAG` is queue based, not simply “previous row” in every context. A simple
  unconditional assignment can map to `Series.shift`, but conditional calls need
  diagnostics or a SAS-compatible queue helper.
- Random-number conversion needs explicit seed and stream policy.
- Distribution functions need argument-order tests because SAS function
  signatures and SciPy distribution APIs do not always line up directly.
- Matrix and optimization code should be classified separately from pandas
  transformations.
