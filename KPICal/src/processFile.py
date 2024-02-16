import os
import pandas as pd
from readData import read_site
from renameColumn import rename
from normalizeData import normalize
from calculateExpected import calculate_expected
from calculateWithExclusions import calculate_with_exclusions
from Summary import update_summary
import Summary


class ProcessContext:
    def __init__(self, base_output_directory, year_month, site_name):
        self.base_output_directory = base_output_directory
        self.year_month = year_month
        self.site_name = site_name


def sum_inv(df, col_name):
    inverter_cols = [col for col in df.columns if col.startswith("Inverter")]
    sum_column = df[inverter_cols].sum(axis=1)
    df = pd.concat([df, sum_column.rename(col_name)], axis=1)
    return df


def save_processed_data(context, folder_name, df):
    outdir_processed_data = os.path.join(
        context.base_output_directory, folder_name, context.year_month
    )
    os.makedirs(outdir_processed_data, exist_ok=True)
    df.to_csv(
        os.path.join(outdir_processed_data, f"{context.site_name}.csv"), index=False
    )


def calculate_and_save_availability(
    selected_actual, expected_data, folder_name, context
):
    selected_expected = expected_data[["Timestamp", "Expected Sum"]]
    merged = pd.merge(selected_actual, selected_expected, on="Timestamp", how="inner")
    merged["Availability %"] = merged["Actual Sum"] / merged["Expected Sum"] * 100
    outdir_availability = os.path.join(
        context.base_output_directory, folder_name, context.year_month
    )
    os.makedirs(outdir_availability, exist_ok=True)
    merged.to_csv(
        os.path.join(outdir_availability, f"{context.site_name}.csv"), index=False
    )
    monthly_availability = merged["Availability %"].mean()
    return monthly_availability


def process_file(file_path, base_output_directory, input_method):
    site_name = file_path.split("_")[-1].replace(".csv", "")
    year_month = file_path.split("_")[0].split("/")[-1]
    context = ProcessContext(base_output_directory, year_month, site_name)

    sitedata = read_site(file_path)
    actual_data = sitedata.pipe(rename).pipe(normalize)
    expected_data_v1 = calculate_expected(actual_data)
    expected_data_v2 = calculate_with_exclusions(actual_data, site_name, input_method)

    # Add a sum column to DataFrames
    actual_data = sum_inv(actual_data, "Actual Sum")
    expected_data_v1 = sum_inv(expected_data_v1, "Expected Sum")
    expected_data_v2 = sum_inv(expected_data_v2, "Expected Sum")

    # Save Processed data
    save_processed_data(context, "expected_data_v1", expected_data_v1)
    save_processed_data(context, "expected_data_v2", expected_data_v2)

    selected_actual = actual_data[["Timestamp", "POA Irradiance", "Actual Sum"]]
    monthly_availability_1 = calculate_and_save_availability(
        selected_actual, expected_data_v1, "availability_v1", context
    )
    monthly_availability_2 = calculate_and_save_availability(
        selected_actual, expected_data_v2, "availability_v2", context
    )

    Summary.summary1 = update_summary(
        Summary.summary1, site_name, year_month, monthly_availability_1
    )

    Summary.summary2 = update_summary(
        Summary.summary2, site_name, year_month, monthly_availability_2
    )
