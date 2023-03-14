
### Input CSV Files

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