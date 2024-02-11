import os
import pandas as pd
from readData import read_site
from renameColumn import rename
from normalizeData import normalize
from calculateExpected import calculate_expected
from Summary import update_summary
import Summary


def sum_inv(df, col_name):
    inverter_cols = [col for col in df.columns if col.startswith("Inverter")]
    sum_column = df[inverter_cols].sum(axis=1)
    df = pd.concat([df, sum_column.rename(col_name)], axis=1)
    return df


def process_file(file_path, base_output_directory):
    site_name = file_path.split("_")[-1].replace(".csv", "")
    year_month = file_path.split("_")[0].split("/")[-1]

    sitedata = read_site(file_path)
    actual_data = sitedata.pipe(rename).pipe(normalize)
    expected_data = calculate_expected(actual_data)
    actual_data = sum_inv(actual_data, "Actual Sum")
    expected_data = sum_inv(expected_data, "Expected Sum")

    outdir_processed_data = os.path.join(
        base_output_directory, "expected_data", year_month
    )
    os.makedirs(outdir_processed_data, exist_ok=True)
    expected_data.to_csv(
        os.path.join(outdir_processed_data, f"{site_name}.csv"), index=False
    )

    selected_actual = actual_data[["Timestamp", "POA Irradiance", "Actual Sum"]]
    selected_expected = expected_data[["Timestamp", "Expected Sum"]]
    merged = pd.merge(selected_actual, selected_expected, on="Timestamp", how="inner")
    merged["Availability %"] = (
        merged["Actual Sum"] / merged["Expected Sum"] * 100
    ).round(2)
    outdir_availability = os.path.join(
        base_output_directory, "availability_trend", year_month
    )
    os.makedirs(outdir_availability, exist_ok=True)
    merged.to_csv(os.path.join(outdir_availability, f"{site_name}.csv"), index=False)

    monthly_availability = merged["Availability %"].mean()
    Summary.summary = update_summary(
        Summary.summary, site_name, year_month, monthly_availability
    )
