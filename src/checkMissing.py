import pandas as pd
from getInfo import log, get_info


def check_missing_irradiance(df):
    log("\nI.\n")
    missing_rows = df[df["POA Irradiance"].isna()]
    if not missing_rows.empty:
        df["POA Irradiance"].fillna(-999, inplace=True)
        missing_rows = df.loc[missing_rows.index]

        log(
            f"Detected missing values in the 'POA Irradiance' column.\n"
            f"All have been filled with a placeholder value of -999."
        )

        missing_during_day = missing_rows[missing_rows["Day/Night"] == "Day"]
        if not missing_during_day.empty:
            log(
                f"Here are missing values during the daytime.(Only showing the first 20 records.)\n"
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"\n{get_info(missing_during_day)}"
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
    condition_value = 100
    columns_to_check = ["Temperature", "Wind Speed"]
    for col in columns_to_check:
        missing_rows = df[df[col].isna()]
        df[col].fillna(-999, inplace=True)
        condition_rows = missing_rows[missing_rows["POA Irradiance"] >= condition_value]
        if not condition_rows.empty:
            log(
                f"Detected missing values in the {col} column when {'POA Irradiance'} >= {condition_value}.\n"
                f"These have been filled with a placeholder value of -999. (Only showing the first 20 records.)\n"
                f"There is no need to document this discrepancy in the 'Data Issues' spreadsheet.\n"
                f"Details of missing rows:\n{get_info(condition_rows)}"
            )

        else:
            log(
                f"All good! When {'POA Irradiance'} >= {condition_value}, the {col} column has no missing values .\n"
            )

    return df


def check_and_autofill_Meter(df):
    log("\nIII.\n")

    missing_rows = df[df["Meter Power"].isna()]
    df["Meter Power"].fillna(-999, inplace=True)
    condition_rows = missing_rows[
        (missing_rows["POA Irradiance"] > 0)
        | (
            (missing_rows["POA Irradiance"] == -999)
            & (missing_rows["Day/Night"] == "Day")
        )
    ]

    inverter_cols = [col for col in df.columns if col.startswith("Inverter_")]

    filled = []
    unfilled = []

    if not condition_rows.empty:
        log(f"Detected missing 'Meter Power' values during daytime.")
        for index, row in condition_rows.iterrows():
            if not row[inverter_cols].isna().any():
                df.loc[index, "Meter Power"] = row[inverter_cols].sum()
                filled.append(df.loc[index])
            else:
                unfilled.append(df.loc[index])

        filled_df = pd.DataFrame(filled)
        unfilled_df = pd.DataFrame(unfilled)

        if not filled_df.empty:
            log(
                f"The missing 'Meter Power' values in the following rows have been auto-filled based on the sum of inverter values.(Only showing the first 20 records.)\n"
                f"{get_info(filled_df)}"
            )

        if not unfilled_df.empty:
            unfilled_df["Date"] = unfilled_df["Timestamp"].dt.date
            missing_by_day = unfilled_df.groupby("Date").size()
            missing_dates = missing_by_day.index.tolist()
            missing_dates_str = "\n".join(
                [f"{date}: {count} missing" for date, count in missing_by_day.items()]
            )
            log(
                f"The missing 'Meter Power' values in the following rows cannot be auto-filled due to missing inverter values.(Only showing the first 20 records.)\n"
                f"These have been filled with a placeholder value of -999.\n"
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"{get_info(unfilled_df)}\n"
                f"\nSummary of missing dates:\n{missing_dates_str}\n"
            )

            return df, missing_dates

    else:
        log("All good! The 'Meter Power' column has no missing values during daytime.")

    return df, None


def check_and_autofill_inverter_and_voltage(df):
    log("\nIV.\n")
    inverter_col = [col for col in df.columns if col.startswith("Inverter_")]
    columns_to_check = ["Meter Voltage"] + inverter_col
    missing_condition = df[columns_to_check].isna().any(axis=1)
    fill_condition = ((df["POA Irradiance"] != -999) & (df["POA Irradiance"] <= 0)) | (
        (df["Meter Power"] != -999) & (df["Meter Power"] <= 0)
    )
    important_condition = (df["POA Irradiance"] > 0) | (
        (df["POA Irradiance"] == -999) & (df["Day/Night"] == "Day")
    )
    mask_fill1 = missing_condition & fill_condition
    sum_fill1 = mask_fill1.sum()

    if sum_fill1 > 0:
        df.loc[mask_fill1, columns_to_check] = df.loc[
            mask_fill1, columns_to_check
        ].fillna(0)

    mask_still_missing_inverter = df[inverter_col].isna().any(axis=1)
    mask_ratio = (round(df["Meter Power"] / (df[inverter_col].sum(axis=1))) == 1) | (
        df["Meter Power"] <= (df[inverter_col].sum(axis=1))
    )
    mask_fill2 = mask_still_missing_inverter & mask_ratio
    sum_fill2 = mask_fill2.sum()
    if sum_fill2 > 0:
        df.loc[mask_fill2, inverter_col] = df.loc[mask_fill2, inverter_col].fillna(0)

    total_fill = sum_fill1 + sum_fill2

    if total_fill > 0:
        log(
            f"{total_fill} rows with missing voltage or inverter values have been auto-filled with 0."
        )

        if not df[important_condition & (mask_fill1 | mask_fill2)].empty:
            log(
                f"Here are filled records within the daytime.(Only showing the first 20 records.)\n"
                f"{get_info(df[important_condition & (mask_fill1 | mask_fill2)])}"
            )
        else:
            log("There were no records filled out during the daytime.")

    important_still_missing = df[
        df[columns_to_check].isna().any(axis=1) & important_condition
    ]

    if not important_still_missing.empty:
        log(
            f"\nThe missing daytime voltage or inverter values in the following rows cannot be filled.(Only showing the first 20 records.)\n"
            f"For more insights and to cross-verify, please refer to the relevant work order records.\n"
            f"{get_info(important_still_missing)}"
        )
    else:
        log("\nAll good! No more missing voltage or inverter values during daytime!")


def missing(df):
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
