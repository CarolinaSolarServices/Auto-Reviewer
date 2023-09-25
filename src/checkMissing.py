import pandas as pd
from getInfo import log, get_info


def check_missing_irradiance(df):
    log("\nI.\n")
    missing_rows = df[df["POA Irradiance"].isna()]
    if not missing_rows.empty:
        df["POA Irradiance"].fillna(-999, inplace=True)
        missing_rows = df.loc[missing_rows.index]

        info = get_info(missing_rows)
        log(
            f"Detected missing values in the 'POA Irradiance' column.\n"
            f"These have been filled with a placeholder value of -999.\n"
            f"Details of all missing rows:\n{info}"
        )

        missing_during_day = missing_rows[missing_rows["Day/Night"] == "Day"]
        if not missing_during_day.empty:
            info_day = get_info(missing_during_day)
            log(
                f"\nHere are missing values during the daytime.\n"
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"\n{info_day}"
            )

        else:
            log(
                f"It seems that all the missing values occur during nighttime hours.\n"
                f"These nighttime omissions are not critical and can be safely overlooked for most analyses."
                f"Therefore, there is no need to document this discrepancy in the 'Data Issues' spreadsheet.\n"
            )

    else:
        log("All good! The 'POA Irradiance' column has no missing values.")

    return df


def check_and_autofill_temperature_and_wind(df):
    log("\nII.\n")
    condition_value = 100
    columns_to_check = ["Temperature", "Wind Speed"]
    for col in columns_to_check:
        condition_rows = df[df["POA Irradiance"] >= condition_value]
        missing_rows = condition_rows[condition_rows[col].isna()]
        if not missing_rows.empty:
            df.loc[missing_rows.index, col] = -999
            missing_info = get_info(missing_rows)
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
    missing_meters = df[
        (df["Meter Power"].isna())
        & (
            (df["POA Irradiance"] > 0)
            | ((df["POA Irradiance"].isna()) & (df["Day/Night"] == "Day"))
        )
    ]

    inverter_cols = [col for col in df.columns if col.startswith("Inverter_")]

    filled = []
    unfilled = []

    if not missing_meters.empty:
        log(f"Detected missing 'Meter Power' values during daytime.")
        for index, row in missing_meters.iterrows():
            if not row[inverter_cols].isna().any():
                df.loc[index, "Meter Power"] = row[inverter_cols].sum()
                filled.append(df.loc[index])
            else:
                df.loc[index, "Meter Power"] = -999
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

            return missing_dates

    else:
        log("All good! The 'Meter Power' column has no missing values during daytime.")

    return None


def missing(df):
    return (
        df.pipe(check_missing_irradiance)
        .pipe(check_and_autofill_temperature_and_wind)
        .pipe(check_and_autofill_Meter)
    )


def check_and_autofill_inverter_and_voltage(df, off_dates):
    log("\nV.\n")
    df["Date"] = df["Timestamp"].dt.date
    mask = df["Date"].isin(off_dates)
    columns_to_check = ["Meter Voltage"] + [
        col for col in df.columns if col.startswith("Inverter_")
    ]

    missing_index = df[mask & df[columns_to_check].isna().any(axis=1)].index
    df.loc[mask, columns_to_check] = df.loc[mask, columns_to_check].fillna(0)

    if not missing_index.empty:
        log(
            f"The missing 'Inverter' and 'Meter Voltage' values in the following rows have been auto-filled with 0.\n"
            f"{get_info(df.loc[missing_index].drop(columns = 'Date'))}"
        )

    else:
        log(
            f"No missing 'Inverter' and 'Meter Voltage' values detected for the off days."
        )

    return df
