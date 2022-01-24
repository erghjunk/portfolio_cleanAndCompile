"""
this merges .csv files that have the SAME structure
and includes a source file column
"""


def merge(path):

    import os
    import glob
    import pandas as pd

    all_files = glob.glob(os.path.join(path, "*.csv"))

    all_df = []
    for f in all_files:
        df = pd.read_csv(f, sep=',')
        df['file'] = f.split('/')[-1]
        all_df.append(df)

    merged_df = pd.concat(all_df, ignore_index=True, sort=False)

    merged_df.to_csv(path + r'\merged_csv_files.csv')
