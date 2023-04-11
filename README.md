[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.2.x&color=orange)

# CCSR Grouper

## 🔗  Quick Links
- [Docs](https://tuva-health.github.io/the_tuva_project/#!/overview): Learn about the Tuva Project data model
- [Knowledge Base](https://thetuvaproject.com/docs/intro): Learn about claims data fundamentals and how to do claims data analytics
<br/><br/>

## 🧰  What does this package do?


The [Clinical Classications Software Refined (CCSR)](https://hcup-us.ahrq.gov/toolssoftware/ccsr/ccs_refined.jsp) SAS programs produce mappings from granular ICD-10 codes to clinically useful categories, and this dbt project creates sql tables mapped according to the same logic. In place of the vertical/horizontal terminology from the CCSR, I've used long/wide in the modern data parlance. Here's how the models relate to the SAS outputs:

* **PRCCSR - Output Option 1, Vertical Output File:** `ccsr__long_procedure_category`. This table includes description columns provided by the CCSR seed file but not defined in the the SAS program.
* **PRCCSR - Output Option 2, Horizontal Output File:** `ccsr__wide_procedure_category`
* **DXCCSR - Output Option 1, Vertical Output File:** `ccsr__long_condition_category`. This table includes description columns provided by the CCSR seed file but not defined in the the SAS program.
* **DXCCSR - Output Option 2, Horizontal Output File:** `ccsr__wide_condition_category`
* **DXCCSR - Output Option 3, Optional File with Default Assignment for Principal or First-Listed Diagnosis:** `ccsr__singular_condition_category`

### Notes on Diagnosis Categories & Defaults

Each ICD-10-CM code is mapped to as many as 6 CCSR categories, though 88.3% of all codes are mapped to only one. This means that given record with `m` ICD-10 each with `n` categories, each record  will have m*n rows in the `ccsr__long_condition_category`.

The CCSR's includes a "default category" for each ICD-10-CM code that allows us to reduce each ICD-10-CM code to a single category. The CCSR differentiates between default categories for inpatient and outpatient records, and the SAS script exposes the record type as a configuration option. We expose these as boolean fields in `ccsr__long_condition_category` - `is_ip_default_category` and `is_op_default_category` for inpatient and outpatient, respectively. By default, the dag will run as inpatient data - this can be configured via the `record_type` variable.

ICD-10-PCS codes map to only one category each, and so have no rank or default.

### Notes on Wide Tables

We opted against running test against each of the 500+ columns in the wide tables, and instead validated a sample of output created by generating wide tables from Medicare SAF data. The following validation script was used to ensure no out of band values - 0,1,2,3 for the wide condition table, and 0 or 1 for wide procedures.

```
import pandas as pd


cond = pd.read_csv('wide_condition.csv')

# remove columns that aren't encoded
column_mask = ~cond.columns.isin(['CLAIM_ID', 'PATIENT_ID','DXCCSR_VERSION'])

print(cond.loc[: , column_mask].max().unique())
print(cond.loc[: , column_mask].min().unique())

proc = pd.read_csv('wide_proc.csv')

# remove columns that aren't encoded
column_mask = ~proc.columns.isin(['ENCOUNTER_ID', 'PATIENT_ID','PRCCSR_VERSION'])

print(proc.loc[: , column_mask].max().unique())
print(proc.loc[: , column_mask].min().unique())
```

### A Note On CSV Seed Files

We encounted issues with the use of mixed quote characters in the CSV files provided with the CCSR SAS programs. We used the following python script to clean the files:

```
import pandas as pd

FILES = ["DXCCSR_v2023-1/DXCCSR_v2023-1.csv", "PRCCSR_v2023-1/PRCCSR_v2023-1.csv"]


def ccsr_csv_cleaner(df, output_name):

    clean_columns = []

    for col in df.columns:
        col_clean = col.strip("'").lower().replace(" ", "_").replace("-", "_")
        clean_columns.append(col_clean)
    df.columns = clean_columns

    quoted_cols = df.columns[[not i for i in df.columns.str.endswith("_description")]]
    df[quoted_cols] = df[quoted_cols].apply(lambda x: x.str.strip("'"))
    df.to_csv(output_name)
    print(f"Saved file: {output_name}")


for file in FILES:
    output_name = f"{file[:6].lower()}_v2023_1_cleaned_map.csv"
    df = pd.read_csv(file)
    ccsr_csv_cleaner(df, output_name)
```
<br/><br/>

## 🔌  Supported Databases and dbt Versions

This package has been tested on: 
- Snowflake

This package supports dbt version `1.4.x` or higher.
<br/><br/>

## 🙋🏻‍♀️ How is this package maintained and how do I contribute?

The Tuva Project team maintaining this package **only** maintains the latest version of the package. We highly recommend you stay consistent with the latest version.

Have an opinion on the mappings? Notice any bugs when installing and running the package? If so, we highly encourage and welcome feedback! While we work on a formal process in Github, we can be easily reached in our Slack community.
<br/><br/>

## 🤝 Join our community!

Join our growing community of healthcare data practitioners in [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!

