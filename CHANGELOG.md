# Changelog

## 3.0.0

### Breaking changes

- Removed `ccsr__singular_condition_category` and the `record_type` variable.
  The package no longer assigns a setting-specific default category to a
  principal or first-listed diagnosis. All AHRQ diagnosis-category mappings
  remain available in `ccsr__long_condition_category`.
- Migrate downstream references before upgrading. dbt leaves the old singular
  table in the warehouse; remove it after migrating its consumers so that it
  cannot be mistaken for a refreshed output.

### Retained behavior

- The other five final outputs and both mapping helpers retain their SQL.
- Diagnosis mappings retain both AHRQ default-category flags as reference
  metadata. These flags do not establish a diagnosis's care setting or role.
- DXCCSR and PRCCSR remain at AHRQ v2027.1. The package reuses mapping asset
  snapshot `2.0.0`; no new mapping files are required.
- Tuva Core remains pinned to `v1.0.0` in the integration project and
  installation examples.

See the [upgrade instructions](README.md#upgrade-from-20) for migration details.
