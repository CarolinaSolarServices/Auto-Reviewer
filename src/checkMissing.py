from getInfo import log, get_info
import numpy as np
import pandas as pd
import Summary


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
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.(Only showing the first 20 records.)\n"
                f"{get_info(df.loc[condition_day_missing])}"
            )
        else:
            log(
                f"It seems that all the missing values occur during night time hours.\n"
            )
    else:
        log("All good! The 'POA Irradiance' column has no missing values.")

    # If existing Irradiance > 1, modify any corresponding "Day/Night" info to "Day".
    condition_day_irr = df["POA Irradiance"] > 1
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
                f"Details of these missing rows:\n{get_info(df.loc[condition_important_missing])}"
            )

        else:
            log(
                f"All good! When {'POA Irradiance'} >= {CONDITION_VALUE}, the {col} column has no missing values .\n"
            )

    return df


def check_and_autofill_Meter(df):
    log("\nIII.\n")
    condition_missing = df["Meter Power"].isna()
    condition_day = df["Day/Night"] == "Day"
    condition_day_missing = condition_missing & condition_day

    inverter_cols = [col for col in df.columns if col.startswith("Inverter_")]
    condition_missing_inverter = df[inverter_cols].isna().any(axis=1)
    condition_can_be_filled = condition_missing & ~condition_missing_inverter
    condition_cannot_be_filled = condition_missing & condition_missing_inverter

    df.loc[condition_missing, "Meter Power"] = -999
    if condition_day_missing.sum() > 0:
        Summary.production_status = "x"
        log(
            f"Detected {condition_day_missing.sum()} missing 'Meter Power' values during daytime."
        )

        if condition_can_be_filled.sum() > 0:
            df.loc[condition_can_be_filled, "Meter Power"] = df.loc[
                condition_can_be_filled, inverter_cols
            ].sum(axis=1)
            log(
                f"{condition_can_be_filled.sum()} rows with missing 'Meter Power' have been auto-filled based on the sum of inverter values.\n"
                f"Only showing the first 20 records here.\n"
                f"{get_info(df.loc[condition_can_be_filled])}"
            )

        if condition_cannot_be_filled.sum() > 0:
            unfilled = df.loc[condition_cannot_be_filled].copy()
            unfilled["Date"] = unfilled["Timestamp"].dt.date
            missing_by_day = unfilled.groupby("Date").size()
            missing_dates = missing_by_day.index.tolist()
            missing_dates_str = "\n".join(
                [f"{date}: {count} missing" for date, count in missing_by_day.items()]
            )
            log(
                f"The missing 'Meter Power' values in the following rows cannot be auto-filled due to missing inverter values.(Only showing the first 20 records.)\n"
                f"These have been filled with a placeholder value of -999.\n"
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"{get_info(unfilled)}\n"
                f"\nSummary of missing dates:\n{missing_dates_str}\n"
            )

            return df, missing_dates

        else:
            log("All good now! No more missing values in the 'Meter Power' columm.")

    else:
        log("All good! The 'Meter Power' column has no missing values during daytime.")

    return df, None


def check_and_autofill_inverter_and_voltage(df):
    log("\nIV.\n")

    # Initial Conditions
    inverter_cols = [col for col in df.columns if col.startswith("Inverter_")]
    cols_all = ["Meter Voltage"] + inverter_cols
    condition_missing_all = df[cols_all].isna().any(axis=1)
    condition_day = df["Day/Night"] == "Day"
    condition_night = df["Day/Night"] == "Night"

    # Step 1
    # A. Fill any voltage or inverter missings with -999 when the corresponding meter power itself is missing(-999).
    condition_definite_missing = df["Meter Power"] == -999
    condition_can_be_filled = condition_missing_all & condition_definite_missing
    if condition_can_be_filled.sum() > 0:
        df.loc[condition_can_be_filled, cols_all] = df.loc[
            condition_can_be_filled, cols_all
        ].fillna(-999)
        log(
            f"{condition_can_be_filled.sum()} rows with missing voltage or inverter values have been auto-filled with -999 due to the missing Meter Power."
            f"Details of filled rows (only showing the first 20 records here):\n"
            f"{get_info(df[condition_can_be_filled])}"
        )

    # B. Fill missing values with 0 when the site is off.
    # The site should be off during night time or when meter power is below 0.
    condition_meter_off = (df["Meter Power"] != -999) & (df["Meter Power"] <= 0)
    condition_site_off = condition_meter_off | condition_night
    condition_can_be_filled = condition_missing_all & condition_site_off

    if condition_can_be_filled.sum() > 0:
        df.loc[condition_can_be_filled, cols_all] = df.loc[
            condition_can_be_filled, cols_all
        ].fillna(0)
        log(
            f"{condition_can_be_filled.sum()} rows with missing voltage or inverter values have been auto-filled with 0 since the site is off.\n"
        )
        if (condition_can_be_filled & condition_day).sum() > 0:
            log(
                f"Here are filled records within the daytime.(Only showing the first 20 records.)\n"
                f"{get_info(df[condition_can_be_filled & condition_day])}"
            )
        else:
            log(
                f"It seems that all the missing values occur during night time hours.\n"
            )

    # Step 2
    # Fill voltage missings by the mean of existing voltages values when all inverters are reporting.
    condition_missing_vol = df["Meter Voltage"].isna()
    condition_missing_inv = df[inverter_cols].isna().any(axis=1)
    condition_inv_all_reporting = ~condition_missing_inv
    condition_can_be_filled = condition_missing_vol & condition_inv_all_reporting
    if condition_can_be_filled.sum() > 0:
        average_voltage = round(df["Meter Voltage"].mean(), 6)
        df.loc[condition_can_be_filled, "Meter Voltage"] = df.loc[
            condition_can_be_filled, "Meter Voltage"
        ].fillna(average_voltage)
        log(
            f"{condition_can_be_filled.sum()} rows with missing voltage have been auto-filled with the estimate mean of existing voltage values since all inverters are reporting normally.\n"
            f"Only showing the first 20 records here:\n"
            f"{get_info(df[condition_can_be_filled])}"
        )

    # Step 3
    # A. If meter power is less than the sum of inverter values and the difference is within a range, the missing inverters can be off -> fill with 0
    condition_missing_inv = df[inverter_cols].isna().any(axis=1)
    inverter_sum = df[inverter_cols].sum(axis=1)
    condition_inv_off = (
        (df["Meter Power"] != -999)
        & (df["Meter Power"] <= inverter_sum)
        & ((inverter_sum != 0) & (round(df["Meter Power"] / inverter_sum) == 1))
    )
    condition_can_be_filled = condition_missing_inv & condition_inv_off
    if condition_can_be_filled.sum() > 0:
        df.loc[condition_can_be_filled, inverter_cols] = df.loc[
            condition_can_be_filled, inverter_cols
        ].fillna(0)
        log(
            f"Missing inverter values in {condition_can_be_filled.sum()} rows with have been auto-filled with 0 since these inverters should be off.\n"
        )

        if (condition_can_be_filled & condition_day).sum() > 0:
            log(
                f"Here are filled records within the daytime.(Only showing the first 20 records.)\n"
                f"{get_info(df[condition_can_be_filled & condition_day])}"
            )
        else:
            log(
                f"It seems that all the missing values occur during night time hours.\n"
            )

    # B. If meter power is larger than the sum of inverter values and the difference is within a range, some missing inverters can be on -> fill with 1
    condition_missing_inv = df[inverter_cols].isna().any(axis=1)
    filled = []
    unfilled = []
    for index, row in df.loc[condition_missing_inv].iterrows():
        INV_sum = row[inverter_cols].sum()
        non_missing_count = row[inverter_cols].notna().sum()
        if (row["Meter Power"] > INV_sum) & (non_missing_count > 0):
            missing_count = row[inverter_cols].isna().sum()

            INV_avg = INV_sum / (non_missing_count)

            try:
                if INV_avg == 0:
                    raise ZeroDivisionError("INV_avg is zero, cannot divide by zero.")
                # print(row["Meter Power"])
                # print(INV_avg)
                estimate_on_count = round((row["Meter Power"] * 1.02 / INV_avg))
                to_be_filled_count = (
                    min(estimate_on_count, (missing_count + non_missing_count))
                    - non_missing_count
                )

                if to_be_filled_count == missing_count:
                    df.loc[index, inverter_cols] = row[inverter_cols].fillna(1)
                    filled.append(index)
                else:
                    unfilled.append(index)

            except ZeroDivisionError:
                unfilled.append(index)

    if len(filled) > 0:
        log(
            f"{len(filled)} rows with missing inverter values have been auto-filled with 1."
            f"Only showing the first 20 records here.\n"
            f"{get_info(df.loc[filled])}"
        )
    if len(unfilled) > 0:
        log(
            f"{len(unfilled)} rows with missing inverter values should have more inverters on."
            f"But it is hard to decide how many more inverters should be on based on the data."
            f"Showing the first 20 records during daytime here for your reference.\n"
            f"{get_info(df.loc[unfilled])}"
        )

    condition_missing_all = df[cols_all].isna().any(axis=1)
    condition_important_still_missing = condition_missing_all & condition_day
    if condition_important_still_missing.sum() > 0:
        log(
            f"Besides, there are still {condition_important_still_missing .sum() } rows with missing voltage or inverter values cannot be filled.\n"
            f"Only showing the first 20 records here.\n"
            f"{get_info(df[condition_important_still_missing])}"
            f"For more insights and to cross-verify, please refer to the relevant work order records.\n"
        )
    else:
        log("\nAll good! No more missing voltage or inverter values during daytime!")


def missing(df):
    # Replace all " - " with NaN before handling missings.
    df.replace(" - ", np.nan, inplace=True)
    df = df.pipe(check_missing_irradiance).pipe(check_and_autofill_temperature_and_wind)
    df, missing_dates = df.pipe(check_and_autofill_Meter)
    df = check_and_autofill_inverter_and_voltage(df)

    return missing_dates
