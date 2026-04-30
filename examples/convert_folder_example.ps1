# Convert a folder of SAS programs into a mirrored Python output folder.
# Run from the repository root:
#   .\examples\convert_folder_example.ps1

param(
    [string]$SourceRoot = "examples\input_repo",
    [string]$OutputRoot = "examples\generated_pandas",
    [ValidateSet("pandas", "pyspark", "databricks")]
    [string]$Target = "pandas",
    [switch]$Strict
)

$ErrorActionPreference = "Stop"

$argsList = @("scripts\convert_sas_folder.py", $SourceRoot, $OutputRoot, "--target", $Target)
if ($Strict) {
    $argsList += "--strict"
}

python @argsList