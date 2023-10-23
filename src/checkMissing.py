from getInfo import log, get_info
import numpy as np


# def get_missing(col, df):
#     condition_nan = df[col].isna()
#     condition_dash = df[col] == "-"
#     condition_missing = (condition_nan | condition_dash).any(axis=1)
#     missing_count = condition_missing.sum()
#     return condition_missing, missing_count


def check_missing_irradiance(df):
    log("\nI.\n")
    condition_missing = df["POA Irradiance"].isna()
    condition_day = df["Day/Night"] == "Day"
    condition_day_missing = condition_missing & condition_day

    if condition_missing.sum() > 0:
        df.loc[condition_missing, "POA Irradiance"] = -999
        log(
            f"Detected {condition_missing.sum() } missing values in the 'POA Irradiance' column.\n"
            f"All have been filled with a placeholder value of -999."
        )
        if condition_day_missing.sum() > 0:
            log(
                f"Detected {condition_day_missing.sum()} missing values during the daytime.\n"
                f"Only showing the first 20 records here."
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"\n{get_info(df.loc[condition_day_missing])}"
            )
        else:
            log(
                f"It seems that all the missing values occur during night time hours.\n"
            )
    else:
        log("All good! The 'POA Irradiance' column has no missing values.")

    return df


def check_and_autofill_temperature_and_wind(df):
    log("\nII.\n")
    CONDITION_VALUE = 100
    cols_to_check = ["Temperature", "Wind Speed"]
    for col in cols_to_check:
        condition_missing = df[col].isna()
        condition_poa = df["POA Irradiance"] >= CONDITION_VALUE
        condition_important_missing = condition_missing & condition_poa
        if condition_missing.sum() > 0:
            df.loc[condition_missing, col] = -999
        if condition_important_missing.sum() > 0:
            log(
                f"Detected {condition_important_missing.sum()} missing values in the {col} column when 'POA Irradiance' >= {CONDITION_VALUE}.\n"
                f"These have been filled with a placeholder value of -999. (Only showing the first 20 records.)\n"
                f"There is no need to document this discrepancy in the 'Data Issues' spreadsheet.\n"
                f"Details of missing rows:\n{get_info(df.loc[condition_important_missing])}"
            )

        else:
            log(
                f"All good! When {'POA Irradiance'} >= {CONDITION_VALUE}, the {col} column has no missing values .\n"
            )

    return df


def check_and_autofill_Meter(df):
    log("\nIII.\n")
    condition_missing = df["Meter Power"].isna()
    condition_day = (df["POA Irradiance"] > 0) | (
        (df["POA Irradiance"] == -999) & (df["Day/Night"] == "Day")
    )
    condition_day_missing = condition_missing & condition_day

    inverter_cols = [col for col in df.columns if col.startswith("Inverter_")]
    condition_missing_inverter = df[inverter_cols].isna().any(axis=1)
    condition_can_be_filled = condition_missing & ~condition_missing_inverter
    condition_cannot_be_filled = condition_missing & condition_missing_inverter

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
                f"Only showing the first 20 records here."
                f"{get_info(df.loc[condition_can_be_filled])}"
            )

        if condition_cannot_be_filled.sum() > 0:
            unfilled = df.loc[condition_cannot_be_filled]
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
    inverter_cols = [col for col in df.columns if col.startswith("Inverter_")]
    cols_to_check = ["Meter Voltage"] + inverter_cols
    condition_site_off = (
        ((df["POA Irradiance"] != -999) & (df["POA Irradiance"] <= 0))
        | (df["POA Irradiance"] == -999) & (df["Day/Night"] == "Night")
        | ((df["Meter Power"] != -999) & (df["Meter Power"] <= 0))
    )
    condition_day = (df["POA Irradiance"] > 0) | (
        (df["POA Irradiance"] == -999) & (df["Day/Night"] == "Day")
    )

    condition_missing = df[cols_to_check].isna().any(axis=1)
    condition_can_be_filled = condition_missing & condition_site_off
    if condition_can_be_filled.sum() > 0:
        df.loc[condition_can_be_filled, cols_to_check].fillna(0, inplace=True)

    condition_still_missing_inverter = df[inverter_cols].isna().any(axis=1)
    condition_inverter_off = (
        round(df["Meter Power"] / (df[inverter_cols].sum(axis=1))) == 1
    ) & (df["Meter Power"] <= (df[inverter_cols].sum(axis=1)))
    condition_can_be_filled_2 = (
        condition_still_missing_inverter & condition_inverter_off
    )
    if condition_can_be_filled_2.sum() > 0:
        df.loc[condition_can_be_filled_2, inverter_cols].fillna(0, inplace=True)

    total_fill = condition_can_be_filled | condition_can_be_filled_2

    if total_fill.sum() > 0:
        log(
            f"{total_fill.sum()} rows with missing voltage or inverter values have been auto-filled with 0."
        )

        if not df[condition_day & total_fill].empty:
            log(
                f"Here are filled records within the daytime.(Only showing the first 20 records.)\n"
                f"{get_info(df[condition_day & total_fill])}"
            )
        else:
            log("There were no records filled out during the daytime.")

    condition_important_still_missing = (
        df[cols_to_check].isna().any(axis=1) & condition_day
    )

    if condition_important_still_missing.sum() > 0:
        log(
            f"{condition_important_still_missing.sum()} rows with missing voltage or inverter values cannot be filled."
            f"Only showing the first 20 records here.\n"
            f"For more insights and to cross-verify, please refer to the relevant work order records.\n"
            f"{get_info(df[condition_important_still_missing])}"
        )
    else:
        log("\nAll good! No more missing voltage or inverter values during daytime!")


def missing(df):
    df.replace(" - ", np.nan, inplace=True)
    # print(f"{get_info(df)}")
    df = df.pipe(check_missing_irradiance).pipe(check_and_autofill_temperature_and_wind)
    df, missing_dates = df.pipe(check_and_autofill_Meter)
    df = check_and_autofill_inverter_and_voltage(df)

    return missing_dates


# def check_and_autofill_inverter_and_voltage(df, off_dates):
#     log("\nV.\n")
#     df["Date"] = df["Timestamp"].dt.date
#     mask = df["Date"].isin(off_dates)
#     columns_to_check = ["Meter Voltage"] + [
#         col for col in df.columns if col.startswith("Inverter_")
#     ]
#     # Indexes of records where there are missing values for voltage and inverter columns,
#     # and the dates fall in the off_dates.
#     missing_off_index = df[mask & df[columns_to_check].isna().any(axis=1)].index
#     df.loc[mask, columns_to_check] = df.loc[mask, columns_to_check].fillna(0)

#     still_missing_index = df[df[columns_to_check].isna().any(axis=1)].index
#     if not missing_off_index.empty:
#         log(
#             f"The missing 'Inverter' and 'Meter Voltage' values in the following rows have been auto-filled with 0, "
#             f"since the work order indicates that the site was non-operational on those particular days. "
#             f"For more insights and to cross-verify, please refer to the relevant work order records.\n"
#             f"{get_info(df.loc[missing_off_index].drop(columns = 'Date'))}\n"
#         )
#     else:
#         log(
#             f"No missing 'Inverter' and 'Meter Voltage' values detected on the off days."
#         )

#     if not still_missing_index.empty:
#         still_missing = df.loc[still_missing_index].drop(columns="Date")
#         important_still_missing = still_missing[
#             (still_missing["POA Irradiance"] > 0)
#             | (
#                 (still_missing["POA Irradiance"] == -999)
#                 & (still_missing["Day/Night"] == "Day")
#             )
#         ]
#         log(
#             f"The missing 'Inverter' and 'Meter Voltage' values in the following rows cannot be handled due to lack of information.\n"
#             f"{get_info(important_still_missing)}"
#         )
#     else:
#         log(
#             f"And No missing 'Inverter' and 'Meter Voltage' values detected for the whole dataset!"
#         )

#     return df
