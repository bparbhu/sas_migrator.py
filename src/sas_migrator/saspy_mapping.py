from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class SaspyConcept:
    name: str
    saspy_direction: str
    reusable_for_migrator: str
    implementation_target: str


SASPY_DATASET_OPTIONS = {
    "where": "Row filter expression",
    "keep": "Column inclusion list",
    "drop": "Column exclusion list",
    "obs": "Last observation limit",
    "firstobs": "First observation offset",
    "format": "SAS format override",
    "encoding": "Input/output encoding",
}


SASPY_TYPE_MAPPING = {
    "sas_numeric_double": {
        "pandas": "float64",
        "pyspark": "DoubleType",
        "notes": "SAS has numeric doubles; date/time/datetime meaning usually comes from formats.",
    },
    "sas_character": {
        "pandas": "string/object",
        "pyspark": "StringType",
        "notes": "Fixed-width SAS character maps to Python strings; preserve labels/formats separately.",
    },
    "sas_date_format": {
        "pandas": "datetime64[ns]",
        "pyspark": "DateType or TimestampType",
        "notes": "SAS stores dates as numeric offsets; formats identify date semantics.",
    },
    "sas_datetime_format": {
        "pandas": "datetime64[ns]",
        "pyspark": "TimestampType",
        "notes": "SAS stores datetimes as numeric seconds; formats identify datetime semantics.",
    },
    "sas_missing": {
        "pandas": "NaN/NaT/pd.NA",
        "pyspark": "null",
        "notes": "Special missing values require explicit profiling if their lettered identity matters.",
    },
}


SASPY_CONCEPTS = [
    SaspyConcept(
        name="SASdata.dsopts",
        saspy_direction="Python object options -> SAS data set option clause",
        reusable_for_migrator="Use the same option vocabulary in IR DatasetRef and emitters.",
        implementation_target="where/keep/drop/obs/firstobs/format/encoding registries",
    ),
    SaspyConcept(
        name="SASdata.contents / columnInfo",
        saspy_direction="Python method -> PROC CONTENTS -> pandas metadata frames",
        reusable_for_migrator="Use as the model for offline metadata/profile JSON.",
        implementation_target="data_io.profile_dataframe and future PROC CONTENTS emitter",
    ),
    SaspyConcept(
        name="sasdata2dataframe / dataframe2sasdata",
        saspy_direction="SAS data set <-> pandas DataFrame through live SAS",
        reusable_for_migrator="Adopt type conversion caveats and optional SAS-backed validation.",
        implementation_target="data_io type handling and equivalence validation",
    ),
    SaspyConcept(
        name="sasdata2parquet",
        saspy_direction="SAS data set -> Parquet via live SAS",
        reusable_for_migrator="Prefer Parquet as durable handoff format for large data.",
        implementation_target="ingest-sas-data command",
    ),
    SaspyConcept(
        name="SASstat/SASml wrappers",
        saspy_direction="Python methods -> SAS analytical PROC code",
        reusable_for_migrator="Use wrapper inventory as PROC-to-Python ecosystem routing hints.",
        implementation_target="proc_registry mappings to statsmodels/sklearn/scipy/spark",
    ),
]


def saspy_mapping_summary() -> dict:
    return {
        "has_reverse_sas_to_python_translator": False,
        "summary": (
            "SASPy is a bridge and a Python-to-SAS code generator. It is useful for "
            "dataset-option vocabulary, type conversion rules, metadata extraction, "
            "Parquet/data exchange strategy, and optional SAS-backed validation, but "
            "it does not provide a static SAS-to-pandas/PySpark translator."
        ),
        "dataset_options": SASPY_DATASET_OPTIONS,
        "type_mapping": SASPY_TYPE_MAPPING,
        "concepts": [asdict(concept) for concept in SASPY_CONCEPTS],
    }
