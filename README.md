[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
![dbt version](https://img.shields.io/badge/dbt-1.10.5%20to%202.x-orange)

# CCSR Grouper

The CCSR Grouper is a standalone dbt package that classifies normalized
ICD-10-CM diagnoses and ICD-10-PCS procedures using the Agency for Healthcare
Research and Quality (AHRQ) [Clinical Classifications Software Refined
(CCSR)](https://hcup-us.ahrq.gov/toolssoftware/ccsr/ccs_refined.jsp). It runs on
top of the Tuva data model and turns granular codes into clinically meaningful
categories for cohorting, utilization analysis, and procedure-pattern
reporting.

This package retains the reviewed DXCCSR and PRCCSR v2023.1 mappings. It is an
open-source Tuva implementation of the published AHRQ mappings; it is not an
official AHRQ product.

## Outputs

The primary package outputs are:

| Model | Grain and use |
| --- | --- |
| `ccsr__long_condition_category` | One row per source condition and mapped diagnosis category. A diagnosis may map to as many as six categories. |
| `ccsr__singular_condition_category` | The inpatient or outpatient default category for each first-listed diagnosis. |
| `ccsr__wide_condition_category` | One row per source-scoped encounter/claim/person record, with one column per diagnosis category and AHRQ-compatible values from 0 through 3. |
| `ccsr__long_procedure_category` | One row per source procedure and mapped procedure category, including ICD-10-PCS ontology attributes. |
| `ccsr__wide_procedure_category` | One row per source-scoped encounter with binary procedure-category columns. |
| `ccsr__procedure_summary` | Source-scoped, claim-linked procedure counts and approach rates by category and root operation. |

The package also materializes `ccsr__dx_vertical_pivot` and
`ccsr__procedure_category_map` as mapping helpers. Column-level definitions,
grains, and tests live in [`models/_model.yml`](models/_model.yml).

## Prerequisites

- dbt `>=1.10.5,<3.0.0`.
- A Tuva connector or another root dbt project that installs a compatible Tuva
  Core version and populates the Tuva Input Layer.
- Built Tuva Core `core__condition` and `core__procedure` models. Procedure
  enrichment also uses Core's `terminology__icd10_pcs_cms_ontology` model.
- Normalized ICD-10-CM and ICD-10-PCS codes in the corresponding Core models.

The production connector/root project owns the Tuva Core dependency. This
package deliberately does not pin or install Core itself, which prevents a
connector and a standalone package from introducing competing Core revisions.
`dbt_utils`, which this package calls directly, is declared in
[`packages.yml`](packages.yml). If you maintain a custom root project without a
Tuva connector, install a compatible Tuva Core revision once in that root
project.

## Installation

Once the package is listed on dbt Hub, add it to the root project's
`packages.yml`:

```yaml
packages:
  - package: tuva-health/ccsr
    version: 0.1.0
```

Then install dependencies:

```shell
dbt deps
```

For a Git-based installation, use the immutable release tag instead:

```yaml
packages:
  - git: "https://github.com/tuva-health/ccsr.git"
    revision: v0.1.0
```

Use either the dbt Hub entry or the Git dependency, not both. Keep Core owned
by the connector or root project in either case.

## Configuration

Defaults are defined in [`dbt_project.yml`](dbt_project.yml). Override package
behavior from the root project with package-scoped variables:

```yaml
vars:
  ccsr:
    record_type: "ip"
    wide_condition_enabled: true
    wide_procedure_enabled: true
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `record_type` | `ip` | Selects the inpatient (`ip`) or outpatient (`op`) default diagnosis category used by `ccsr__singular_condition_category`. |
| `wide_condition_enabled` | `true` | Builds the wide diagnosis-category output. Set to `false` when only long or singular output is needed. |
| `wide_procedure_enabled` | `true` | Builds the wide procedure-category output. Set to `false` when only long or summary output is needed. |
| `ccsr_data_asset_version` | `1.0.0` | Selects the published mapping snapshot. Change only when coordinating against another published CCSR asset version. |

The `dxccsr_version` and `prccsr_version` defaults label the retained AHRQ
mapping version. They are coordinated with the package's published assets and
normally should not be overridden independently.

After the connector and Core models are ready, build the package with its seeds
and tests:

```shell
dbt build --select package:ccsr
```

## Data assets

The package owns three mapping assets:

- DXCCSR v2023.1 code-to-category mappings
- DXCCSR v2023.1 body-system mappings
- PRCCSR v2023.1 code-to-category mappings

The checked-in CSVs are header-only dbt loader contracts. During `dbt seed`,
Tuva Core's shared loader retrieves the contents from the public CCSR asset
path in S3, with equivalent mirrors in GCS and Azure.

Package code and data assets have independent versions. Package release
`0.1.0` intentionally uses the existing `ccsr_data_asset_version: "1.0.0"`
snapshot; the asset version is not inferred from the package version.

## Compatibility

The end-to-end Tuva 1.0 support matrix covers:

- Snowflake
- BigQuery
- Databricks
- Microsoft Fabric
- Redshift
- DuckDB

Release preparation includes full package or integrated execution on
Snowflake, Microsoft Fabric, and DuckDB; targeted execution on Redshift; and
adapter portability review for BigQuery and Databricks. CCSR package SQL has
also received package-level portability review for Microsoft SQL Server and
Amazon Athena, but those adapters are outside the current end-to-end Tuva Core
support matrix and are not claimed as supported Tuva stack targets here.
Because adapter and connector environments differ, validate the package with
your connector and warehouse before promoting it to production.

## Documentation and support

- [Tuva CCSR documentation](https://thetuvaproject.com/data-marts/ccsr)
- [Tuva getting started guide](https://thetuvaproject.com/getting-started)
- [Open an issue](https://github.com/tuva-health/ccsr/issues)
- [Tuva Community Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)

## Contributing

Issues and pull requests are welcome. Please describe the affected CCSR
mapping or output contract, add the smallest dbt-native test that demonstrates
behavioral changes, and report the dbt adapter and version used for validation.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
