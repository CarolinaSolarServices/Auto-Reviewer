from getInfo import log, get_info
import numpy as np
import pandas as pd
import Summary
import renameColumn


def check_missing_irradiance(df):
    log("\nI.\n")
    condition_missing = df["POA Irradiance"].isna()
    condition_day = df["Day/Night"] == "Day"

    condition_day_missing = condition_missing & condition_day

    if condition_missing.sum() > 0:
        # Fill all the missings with -999
        df.loc[condition_missing, "POA Irradiance"] = -999
        log(
            f"Detected {condition_missing.sum() } missing values in the 'POA Irradiance' column.\n"
            f"All have been filled with a placeholder value of -999."
        )

        # Only document the missings during daytime
        if condition_day_missing.sum() > 0:
            Summary.irradiance_status = "x"
            log(
                f"Detected {condition_day_missing.sum()} missing values during the daytime.\n"
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"{get_info(df.loc[condition_day_missing])}\n\n"
            )
        else:
            log(
                f"It seems that all the missing values occur during night time hours.\n"
            )
    else:
        log("All good! The 'POA Irradiance' column has no missing values.")

    # If existing Irradiance > 2, modify any corresponding "Day/Night" info to "Day".
    condition_day_irr = df["POA Irradiance"] > 2
    condition_night_irr = (df["POA Irradiance"] != -999) & (df["POA Irradiance"] <= 1)
    df.loc[condition_day_irr, "Day/Night"] = "Day"
    df.loc[condition_night_irr, "Day/Night"] = "Night"

    return df


def check_and_autofill_temperature_and_wind(df):
    log("\nII.\n")
    CONDITION_VALUE = 100
    cols_to_check = ["Temperature", "Wind Speed"]
    condition_poa = df["POA Irradiance"] >= CONDITION_VALUE
    for col in cols_to_check:
        condition_missing = df[col].isna()
        condition_important_missing = condition_missing & condition_poa
        # Fill all the missings with -999
        if condition_missing.sum() > 0:
            df.loc[condition_missing, col] = -999
            log(
                f"Detected {condition_missing.sum()} missing values in the {col} column.\n"
                f"All have been filled with a placeholder value of -999.\n"
                f"There is no need to document this discrepancy in the 'Data Issues' spreadsheet.\n"
            )

        # Only document the missings when Irradiance >= 100
        if condition_important_missing.sum() > 0:
            log(
                f"Significantly, {condition_important_missing.sum()} of these missings occure when 'POA Irradiance' >= {CONDITION_VALUE}.\n"
                f"Details of these missing rows:\n{get_info(df.loc[condition_important_missing])}\n\n"
            )

        else:
            log(
                f"All good! When {'POA Irradiance'} >= {CONDITION_VALUE}, the {col} column has no missing values .\n"
            )

    return df


def check_and_autofill_Meter(df):
    log("\nIV.\n")
    condition_missing = df["Meter Power"].isna()
    condition_day = df["Day/Night"] == "Day"
    condition_day_missing = condition_missing & condition_day

    inverter_cols = [col for col in df.columns if col.startswith("Inverter_")]
    condition_missing_inverter = df[inverter_cols].isna().any(axis=1)
    condition_no_issue = condition_missing & ~condition_missing_inverter
    condition_with_issue = condition_missing & condition_missing_inverter

    condition_at_least_one_inverter = df[inverter_cols].notna().any(axis=1)
    condition_can_be_filled = condition_missing & condition_at_least_one_inverter

    df.loc[condition_missing, "Meter Power"] = -999
    if condition_day_missing.sum() > 0:
        log(
            f"Detected {condition_day_missing.sum()} missing 'Meter Power' values during daytime."
        )
        if condition_can_be_filled.sum() > 0:
            df.loc[condition_can_be_filled, "Meter Power"] = df.loc[
                condition_can_be_filled, inverter_cols
            ].sum(axis=1)
            log(
                f"{condition_can_be_filled.sum()} rows with missing 'Meter Power' have been auto-filled based on the sum of inverter values.\n"
                f"{get_info(df.loc[condition_can_be_filled])}\n\n"
            )

        if condition_no_issue.sum() > 0:
            log(
                f"{condition_no_issue.sum()} rows with missing 'Meter Power' have all corresponding inverters reporting. No production data issue will be marked for these cases.\n"
                f"{get_info(df.loc[condition_no_issue])}\n\n"
            )

        if condition_with_issue.sum() > 0:
            Summary.production_status = "x"
            with_issue = df.loc[condition_with_issue].copy()
            with_issue["Date"] = with_issue["Timestamp"].dt.date
            missing_by_day = with_issue.groupby("Date").size()
            missing_dates = missing_by_day.index.tolist()
            missing_dates_str = "\n".join(
                [f"{date}: {count} missing" for date, count in missing_by_day.items()]
            )
            log(
                f"The missing 'Meter Power' values in the following rows have been flagged as an issue in the 'Data Issues' spreadsheet for further review, as their corresponding inverters are not fully reporting.\n"
                f"{get_info(with_issue)}\n"
                f"\nSummary of dates that have production data issue:\n{missing_dates_str}\n\n"
            )

            return df, missing_dates

        else:
            log("All good now! No more missing values in the 'Meter Power' columm.")

    else:
        log("All good! The 'Meter Power' column has no missing values during daytime.")

    return df, None


def check_and_autofill_inverter(df):
    log("\nIII.\n")

    inverter_cols = [col for col in df.columns if col.startswith("Inverter_")]
    condition_missing = df[inverter_cols].isna().any(axis=1)
    condition_day = df["Day/Night"] == "Day"
    condition_night = df["Day/Night"] == "Night"

    # Step 1
    # Fill missing inverter values with 0 when the site is off.
    # The site should be off during night time or when meter power is below 0.
    condition_meter_off = df["Meter Power"] <= 0
    condition_site_off = condition_meter_off | condition_night
    condition_can_be_filled = condition_missing & condition_site_off

    if condition_can_be_filled.sum() > 0:
        df.loc[condition_can_be_filled, inverter_cols] = df.loc[
            condition_can_be_filled, inverter_cols
        ].fillna(0)
        log(
            f"{condition_can_be_filled.sum()} rows with missing inverter values have been auto-filled with 0 since the site is off.\n"
        )
        if (condition_can_be_filled & condition_day).sum() > 0:
            log(
                f"Here are filled records within the daytime.\n"
                f"{get_info(df[condition_can_be_filled & condition_day])}\n\n"
            )
        else:
            log(
                f"It seems that all the missing values occur during night time hours.\n"
            )

    # Step 2
    # Fill still missing inverters based on different situations.
    # A. If meter power is less than the sum of inverter values and the difference is within a range, the missing inverters can be off -> fill with 0
    condition_missing = df[inverter_cols].isna().any(axis=1)
    inverter_sum = df[inverter_cols].sum(axis=1)
    condition_inv_off = (df["Meter Power"] <= inverter_sum) & (
        round(inverter_sum / df["Meter Power"]) == 1
    )
    condition_can_be_filled = condition_missing & condition_inv_off
    if condition_can_be_filled.sum() > 0:
        df.loc[condition_can_be_filled, inverter_cols] = df.loc[
            condition_can_be_filled, inverter_cols
        ].fillna(0)
        log(
            f"Missing inverter values in {condition_can_be_filled.sum()} rows have been automatically set to 0, indicating these inverters are off.\n"
            f"This is based on the small difference between the meter power value and the sum of existing inverters, suggesting the missing inverters are inactive.\n"
        )

        if (condition_can_be_filled & condition_day).sum() > 0:
            log(
                f"Here are filled records within the daytime.\n"
                f"{get_info(df[condition_can_be_filled & condition_day])}\n\n"
            )
        else:
            log(
                f"It seems that all the missing values occur during night time hours.\n"
            )

    # B. If meter power is larger than the sum of inverter values and the difference is within a range, some missing inverters can be on -> fill with 1
    # condition_missing = df[inverter_cols].isna().any(axis=1) | (
    #     df[inverter_cols] == 0
    # ).any(axis=1)
    condition_missing = df[inverter_cols].isna().any(axis=1)
    filled_index = []
    unfilled_index = []
    for index, row in df.loc[condition_missing].iterrows():
        INV_sum = row[inverter_cols].sum()
        count_nan = row[inverter_cols].isna().sum()
        count_zero = (row[inverter_cols] == 0).sum()
        count_not_reporting = count_nan + count_zero
        count_reporting = (row[inverter_cols] > 0).sum()
        if (row["Meter Power"] > INV_sum) & (count_reporting > 0):
            INV_avg = INV_sum / count_reporting

            try:
                # if INV_avg == 0:
                #     raise ZeroDivisionError()
                count_estimated_on_total = round((row["Meter Power"] * 1.02 / INV_avg))
                count_estimated_should_have_on = (
                    min(
                        count_estimated_on_total,
                        (count_not_reporting + count_reporting),
                    )
                    - count_reporting
                )

                if count_estimated_should_have_on == count_nan:
                    df.loc[index, inverter_cols] = row[inverter_cols].fillna(1)
                    filled_index.append(index)
                else:
                    missing_indices = row[inverter_cols].index[
                        row[inverter_cols].isna()
                    ]
                    fill_indices = missing_indices[:count_estimated_should_have_on]
                    df.loc[index, fill_indices] = 1
                    df.loc[index, inverter_cols] = df.loc[index, inverter_cols].fillna(
                        0
                    )
                    unfilled_index.append(index)

            except ZeroDivisionError:
                unfilled_index.append(index)

    if len(filled_index) > 0:
        log(
            f"{len(filled_index)} rows with missing inverter values have been auto-filled with 1.\n"
            f"{get_info(df.loc[filled_index])}\n\n"
        )
    if len(unfilled_index) > 0:
        log(
            f"{len(unfilled_index)} rows with missing inverter values should have more inverters on."
            f"But it is hard to automatically decide how many more inverters or which inverters exactly should be on based on the data."
            f"We have automatically filled the first <n> missing inverters with 1, where <n> is the estimated number of inverters expected to be operational but not reporting normally."
            f"{get_info(df.loc[unfilled_index])}\n\n"
        )

    # If there are still missing inverters, document the issue in the summary table.
    condition_missing = df[inverter_cols].isna().any(axis=1)
    condition_high_irradiance = df["Meter Power"] >= 100
    condition_log = condition_missing & condition_high_irradiance
    if condition_log.sum() > 0:
        Summary.inverter_status = "x"

    if condition_missing.sum() > 0:
        log(
            f"There are still {condition_missing.sum() } rows with missing inverter values cannot be automatically filled.\n"
        )
        if (condition_missing & condition_day).sum() > 0:
            log(
                f"Here are still missing records within the daytime.\n"
                f"{get_info(df[condition_missing & condition_day])}\n\n"
            )
        else:
            log("\nGood! No more missing inverter values during daytime!")

    return df


def check_and_autofill_voltage(df):
    log("\nV.\n")
    condition_missing = df["Meter Voltage"].isna()

    # Fill voltage missings by the mean of existing voltages values when the meter power is greater than 0.
    if df["Meter Voltage"].isna().all():
        df["Meter Voltage"].fillna(-999, inplace=True)
        log(
            "Meter Voltage column was completely empty and has been filled with -999.\n"
        )
    else:
        condition_meter_producing = df["Meter Power"] > 0
        condition_can_be_filled = condition_missing & condition_meter_producing
        if condition_can_be_filled.sum() > 0:
            valid_voltages = df["Meter Voltage"][(df["Meter Voltage"] > 0)]
            average_voltage = round(valid_voltages.mean(), 6)
            df.loc[condition_can_be_filled, "Meter Voltage"] = df.loc[
                condition_can_be_filled, "Meter Voltage"
            ].fillna(average_voltage)
            log(
                f"{condition_can_be_filled.sum()} rows with missing voltage have been auto-filled with the estimate mean of existing voltage values since all inverters are reporting normally.\n"
                f"{get_info(df[condition_can_be_filled])}\n\n"
            )

    return df


def count_max_missing_inverter(df):
    inverter_cols = [col for col in df.columns if col.startswith("Inverter_")]
    condition_irr = df["POA Irradiance"] > 100
    missing_counts = df.loc[condition_irr, inverter_cols].isna().sum()
    Summary.max_missing_count = missing_counts.max()

    max_missing_inverters = missing_counts[
        missing_counts == missing_counts.max()
    ].index.tolist()
    original_names = [
        renameColumn.name_mapping[name]
        for name in max_missing_inverters
        if name in renameColumn.name_mapping
    ]
    names_str = ", ".join(original_names)
    Summary.max_missing_inverters = names_str


def check_missing(df):
    # Replace all " - " with NaN before handling missings.
    df.replace(" - ", np.nan, inplace=True)
    df = df.pipe(check_missing_irradiance).pipe(check_and_autofill_temperature_and_wind)
    df = check_and_autofill_inverter(df)
    df, missing_dates = check_and_autofill_Meter(df)
    df = check_and_autofill_voltage(df)
    count_max_missing_inverter(df)

    log(
        "\nFor more insights and to cross-verify, please refer to the relevant work order records.\n"
    )

    return missing_dates
