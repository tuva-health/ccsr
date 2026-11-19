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

This package uses DXCCSR and PRCCSR v2027.1, released by AHRQ on November 10,
2026 and designated for use through September 2027. The diagnosis release
does not include the April 1, 2027 ICD-10-CM update. It is an
open-source Tuva implementation of the published AHRQ mappings; it is not an
official AHRQ product. AHRQ and HHS do not endorse this adaptation and are not
responsible for its content or use.

## Outputs

The primary package outputs are:

| Model | Grain and use |
| --- | --- |
| `ccsr__long_condition_category` | One row per source condition and mapped diagnosis category. A diagnosis may map to as many as six categories. |
| `ccsr__wide_condition_category` | One row per source-scoped encounter/claim/person record, with one column per diagnosis category and values from 0 through 3 based on Core diagnosis ranks. |
| `ccsr__long_procedure_category` | One row per source procedure and mapped procedure category, including ICD-10-PCS ontology attributes. |
| `ccsr__wide_procedure_category` | One row per source-scoped encounter with binary procedure-category columns. |
| `ccsr__procedure_summary` | Source-scoped, claim-linked procedure counts and approach rates by category and root operation. |

The package also materializes `ccsr__dx_vertical_pivot` and
`ccsr__procedure_category_map` as mapping helpers. Column-level definitions,
grains, and tests live in [`models/_model.yml`](models/_model.yml).

Diagnosis mapping retains every category assigned by AHRQ, regardless of care
setting or diagnosis rank. Codes absent from the mapping remain in the long
output with null category fields. The package does not produce AHRQ's optional
single default category for a principal or first-listed diagnosis. That output
requires reliable diagnosis role and inpatient/outpatient context. A source
rank of 1 alone does not establish the principal diagnosis for an encounter.

The `is_ip_default_category` and `is_op_default_category` columns retain AHRQ's
default designations as mapping metadata. They do not identify where a diagnosis
occurred and do not restrict diagnosis or procedure mappings to either setting.
The wide diagnosis output continues to use Core's source diagnosis ranks; use
the long output when ranks are missing or when all mapped conditions are needed.

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

Declare Tuva Core and this package once in the root project's
`packages.yml`. Use the immutable release tags for the 2.1 release:

```yaml
packages:
  - git: "https://github.com/tuva-health/tuva-core.git"
    revision: "v1.0.0"
  - git: "https://github.com/tuva-health/ccsr.git"
    revision: "v2.1.0"
```

After these releases are available on dbt Hub, the equivalent installation is:

```yaml
packages:
  - package: tuva-health/the_tuva_project
    version: 1.0.0
  - package: tuva-health/ccsr
    version: 2.1.0
```

Then install dependencies from the root project:

```shell
dbt deps
```

## Configuration

CCSR models use the `ccsr` schema, or `<tuva_schema_prefix>_ccsr` when a
prefix is configured, under Tuva's schema naming. The root project's schema
naming macro and explicit model overrides still apply. Seed locations are
configured independently. Rebuild CCSR after upgrading to this routing; dbt
does not remove tables left in the previous schema.

Defaults are defined in [`dbt_project.yml`](dbt_project.yml). Override package
behavior from the root project with package-scoped variables:

```yaml
vars:
  ccsr:
    wide_condition_enabled: true
    wide_procedure_enabled: true
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `wide_condition_enabled` | `true` | Builds the wide diagnosis-category output. Set to `false` when only long output is needed. |
| `wide_procedure_enabled` | `true` | Builds the wide procedure-category output. Set to `false` when only long or summary output is needed. |
| `ccsr_data_asset_version` | `2.0.0` | Selects the published mapping snapshot. Change only when coordinating against another published CCSR asset version. |

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

- DXCCSR v2027.1 code-to-category mappings
- DXCCSR v2027.1 body-system mappings
- PRCCSR v2027.1 code-to-category mappings

The checked-in CSVs are header-only dbt loader contracts. During `dbt seed`,
Tuva Core's shared loader retrieves the contents from the public CCSR asset
path in S3, with equivalent mirrors in GCS and Azure.

Package code and data assets have independent versions. This update uses
`ccsr_data_asset_version: "2.0.0"`; the asset version is not inferred from the
package version. The existing `1.0.0` asset snapshot remains available to older
package releases. Do not point the new seed contracts at that older snapshot.

Source archives, deterministic preparation, file hashes, row counts, and
provenance belong to the versioned cloud `_manifest.json` and the
[`tuva-maintenance-skills` data-assets workflow](https://github.com/tuva-health/tuva-maintenance-skills/tree/main/skills/data-assets).
The three seed headers remain unchanged; resource/file names now identify
v2027.1. PRCCSR v2027.1 adds an AHRQ `APPROACH` classification (`OA`, `MI`,
`NC`). That field is intentionally omitted from this package mapping: the
existing `approach` output and procedure summary continue to use the more
detailed CMS ICD-10-PCS ontology from Tuva Core. Ontology coverage for new
procedure codes therefore depends on the Core data assets installed.

## Upgrade from 2.0

Version 2.1 removes `ccsr__singular_condition_category` and the `record_type`
variable. Consumers of the removed model must migrate. The other five output
models retain their SQL, schemas, and classification results. AHRQ mappings
remain at v2027.1 and the mapping asset snapshot remains at `2.0.0`.

1. Replace downstream references to `ccsr__singular_condition_category` with
   analyses based on `ccsr__long_condition_category`. This is not a direct table
   substitution: a diagnosis can map to several categories, and the long table
   includes all diagnosis ranks. Deduplicate people or encounters within each
   category for the analysis; totals across categories can overlap.
2. Remove `record_type` from the root project's variables. It no longer has an
   effect. Do not replace it with a default-category or rank-1 filter unless
   your analysis independently establishes the care setting and diagnosis role.
3. Update the package revision to `v2.1.0`, run `dbt deps`, then run
   `dbt build --select package:ccsr` against the existing Core models.
4. After migrating dependent models and reports, explicitly drop the old
   singular table from its deployed schema. dbt does not delete relations when
   their model files are removed; a leftover table will no longer be refreshed.

## Upgrade from 1.0

Follow the 2.0-to-2.1 migration above as well as the mapping changes below.
The mapping update introduced in 2.0 changes dynamically generated wide schemas
and classification results. Review dependent extracts, BI models, and category
lists before rebuilding.

| Change from AHRQ v2023.1 | DXCCSR | PRCCSR |
| --- | --- | --- |
| ICD codes | 74,549 → 75,915 | 81,597 → 82,510 |
| Active categories / wide category columns | 543 → 554 | 327 → 328 |
| Existing codes with changed category assignments | 347 | 154 |

The diagnosis wide output adds `FAC026`–`FAC031`, `NEO075`–`NEO078`, and
`SYM018`–`SYM019`, and removes `FAC021` (columns use the `dxccsr_` prefix).
The procedure wide output adds `prccsr_esa012`. Diagnosis inpatient defaults
change for 55 existing codes and outpatient defaults for 292. Body-system
names and category-count descriptions are regenerated from the current AHRQ
reference workbook. The long outputs retain their schemas and source-scoped
grains; the singular output is removed in 2.1.

1. Upgrade the CCSR package and remove root-level `dxccsr_version`,
   `prccsr_version`, or `ccsr_data_asset_version` overrides that retain the
   2023.1 / asset 1.0.0 baseline. The new version labels default to `'2027.1'`.
2. Run `dbt deps`, then `dbt build --full-refresh --select package:ccsr` after
   building the connector and Core. Keep both wide outputs enabled when
   checking migration compatibility.
3. Rebuild downstream models, including Semantic Layer models that consume
   CCSR. Root projects must install the updated CCSR package once; a
   downstream package's old dependency pin or version-variable override must
   be reconciled before upgrading.

The new mappings include obesity class codes E66.811–E66.813 and the other
reported newer codes. Z29.89 maps to FAC005, but its inpatient default is
XXX000 (unacceptable principal diagnosis); FAC005 is its outpatient default.
This distinction is retained from AHRQ.

Tuva Core's separate service-category DXCCSR mapping is outside this
standalone update and retains its own version. The CCSR upgrade does not
change service-category classification in Core.

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
