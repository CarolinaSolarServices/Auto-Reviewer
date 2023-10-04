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
            subset = pd.concat(
                [missing_during_day.iloc[:, :10], missing_during_day.iloc[:, -3:]],
                axis=1,
            )
            info_day = get_info(subset)
            log(
                f"Here are missing values during the daytime.\n"
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"\n{info_day}"
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
            missing_info = get_info(condition_rows)
            log(
                f"Detected missing values in the {col} column when {'POA Irradiance'} >= {condition_value}.\n"
                f"These have been filled with a placeholder value of -999.\n"
                f"There is no need to document this discrepancy in the 'Data Issues' spreadsheet.\n"
                f"Details of missing rows:\n{missing_info}"
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
                f"The missing 'Meter Power' values in the following rows have been auto-filled based on the sum of inverter values.\n"
                f"{get_info(filled_df)}"
            )

        if not unfilled_df.empty:
            info = get_info(unfilled_df)
            unfilled_df["Date"] = unfilled_df["Timestamp"].dt.date
            missing_by_day = unfilled_df.groupby("Date").size()
            missing_dates = missing_by_day.index.tolist()
            missing_dates_str = "\n".join(
                [f"{date}: {count} missing" for date, count in missing_by_day.items()]
            )
            log(
                f"The missing 'Meter Power' values in the following rows cannot be auto-filled due to missing inverter values.\n"
                f"These have been filled with a placeholder value of -999.\n"
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"{info}\n"
                f"\nSummary of missing dates:\n{missing_dates_str}\n"
            )

            return df, missing_dates

    else:
        log("All good! The 'Meter Power' column has no missing values during daytime.")

    return df, None


def check_and_autofill_inverter_and_voltage(df):
    log("\nIV.\n")
    columns_to_check = ["Meter Voltage"] + [
        col for col in df.columns if col.startswith("Inverter_")
    ]
    missing_condition = df[columns_to_check].isna().any(axis=1)
    fill_condition = ((df["POA Irradiance"] != -999) & (df["POA Irradiance"] <= 0)) | (
        (df["Meter Power"] != -999) & (df["Meter Power"] <= 0)
    )
    important_condition = (df["POA Irradiance"] > 0) | (
        (df["POA Irradiance"] == -999) & (df["Day/Night"] == "Day")
    )
    # subset = pd.concat([df.iloc[:, :10], df.iloc[:, -3:]], axis=1)
    # log(f"{get_info(subset)}")

    # log("\n")

    if not df[missing_condition & fill_condition].empty:
        df.loc[missing_condition & fill_condition, columns_to_check] = df.loc[
            missing_condition & fill_condition, columns_to_check
        ].fillna(0)

        log(
            f"{(missing_condition & fill_condition).sum()} rows with missing voltage or inverter values have been auto-filled with 0."
        )

        # if not df[missing_condition & fill_condition].empty:
        #     print_df = df[missing_condition & fill_condition]
        subset = pd.concat(
            [df[important_condition].iloc[:, :9], df[important_condition].iloc[:, -3:]],
            axis=1,
        )
        log(f"Here are filled records within the daytime.\n" f"{get_info( subset)}")

    important_still_missing = df[
        df[columns_to_check].isna().any(axis=1) & important_condition
    ]
    if not important_still_missing.empty:
        subset = pd.concat(
            [important_still_missing.iloc[:, :9], important_still_missing.iloc[:, -3:]],
            axis=1,
        )
        log(
            f"\nThe missing voltage or inverter values in the following rows cannot be filled.\n"
            f"For more insights and to cross-verify, please refer to the relevant work order records.\n"
            f"{get_info(subset)}"
        )


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
