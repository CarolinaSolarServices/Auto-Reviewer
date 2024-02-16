import pandas as pd
import numpy as np


# Calculate the average energy produced by the best 20% of inverters
# that have been working non-stop for at least an hour in conditions where POA Irradiance > 50
def compute_avg(df):
    inverter_cols = [col for col in df.columns if col.startswith("Inverter_")]
    # Initialize a Series to store the average value of top 20% inverter values when POA Irradiance > 50
    avg_top_20_series = pd.Series(index=df.index, dtype="float64")
    for index, row in df.iterrows():
        if pd.notnull(row["POA Irradiance"]) and row["POA Irradiance"] > 50:
            candidate_values = row[inverter_cols].dropna()
            positive_candidates = candidate_values[candidate_values > 0]
            if not positive_candidates.empty:
                # 80% of the values in positive_values are less than or equal to the 80th percentile value
                percentile_80 = np.percentile(positive_candidates, 80)
                # Select values greater than or equal to the 80th percentile
                top_20 = positive_candidates[positive_candidates >= percentile_80]
                if not top_20.empty:
                    avg_top_20_series.at[index] = top_20.mean()

    return avg_top_20_series


def process_inverter(df_copy, avg_top_20_series):
    df_copy["Avg_Top_20%"] = avg_top_20_series
    inverter_cols = [col for col in df_copy.columns if col.startswith("Inverter_")]

    for col in inverter_cols:
        mask = (df_copy[col] <= 0) & pd.notnull(df_copy["Avg_Top_20%"])
        df_copy.loc[mask, col] = df_copy.loc[mask, "Avg_Top_20%"]

    return df_copy


def calculate_expected(df):
    return process_inverter(df.copy(deep=True), compute_avg(df))
