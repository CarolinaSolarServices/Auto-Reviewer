import numpy as np
import pandas as pd
from getInfo import log, get_info

name_mapping = {}


def column_temperature(df):
    temperature_cols = [col for col in df.columns if "temperature" in col.lower()]
    if len(temperature_cols) == 1:
        df.rename(columns={temperature_cols[0]: "Temperature"}, inplace=True)
    elif len(temperature_cols) == 0:
        df["Temperature"] = np.nan
    else:
        # Check for columns with "ambient" in their name
        ambient = [col for col in temperature_cols if "ambient" in col.lower()]

        # If there's a column with "ambient", use that. Otherwise, use the first "temperature" column
        col_to_use = ambient[0] if ambient else temperature_cols[0]
        df.rename(columns={col_to_use: "Temperature"}, inplace=True)

        # Drop any other temperature columns to keep the dataframe clean
        cols_to_drop = [col for col in temperature_cols if col != col_to_use]
        df.drop(columns=cols_to_drop, inplace=True)

    return df


def column_wind(df):
    wind = [
        col for col in df.columns if "wind" in col.lower() or "speed" in col.lower()
    ]
    if wind:
        df.rename(columns={wind[0]: "Wind Speed"}, inplace=True)
    else:
        new_wind_col = pd.Series(np.nan, index=df.index, name="Wind Speed")
        df = pd.concat([df, new_wind_col], axis=1)
    return df


def column_voltage(df):
    voltage = [col for col in df.columns if "voltage" in col.lower()]
    if len(voltage) > 1:
        col_to_use = min(voltage, key=lambda col: df[col].isna().sum())
        df.rename(columns={col_to_use: "Meter Voltage"}, inplace=True)
        cols_to_drop = [col for col in voltage if col != col_to_use]
        df.drop(columns=cols_to_drop, inplace=True)
    elif voltage:
        df.rename(columns={voltage[0]: "Meter Voltage"}, inplace=True)
    else:
        df["Meter Voltage"] = np.nan

    return df


def column_others(df):
    keyword_mapping = {
        "Timestamp": ["timestamp"],
        "POA Irradiance": ["poa"],
        "Meter Power": ["meter", "power"],
    }

    rename_mapping = {}
    for new_name, keywords in keyword_mapping.items():
        found = False
        for col in df.columns:
            if all(keyword.lower() in col.lower() for keyword in keywords):
                rename_mapping[col] = new_name
                found = True
                break
        if not found:
            df[new_name] = np.nan

    df.rename(columns=rename_mapping, inplace=True)

    return df


def column_inverter(df):
    known_columns = {
        "Timestamp",
        "POA Irradiance",
        "Meter Voltage",
        "Meter Power",
        "Temperature",
        "Wind Speed",
    }
    inverter_index = 1

    for col in df.columns:
        if col not in known_columns:
            new_name = "Inverter_" + str(inverter_index)
            df.rename(columns={col: new_name}, inplace=True)
            name_mapping[new_name] = col
            inverter_index += 1

    # mapping_df = pd.DataFrame(
    #     list(name_mapping.items()),
    #     columns=["Original Inverter Name", "Converted Inverter Name"],
    # )
    # log(f"NOTES ON INVERTER NAMES:\n{get_info(mapping_df)}")

    return df


def column_reorder(df):
    inverter_columns = sorted(
        (col for col in df.columns if "Inverter" in col),
        key=lambda s: int(s.split("_")[1]),
    )
    columns_order = [
        "Timestamp",
        "POA Irradiance",
        "Temperature",
        "Wind Speed",
        "Meter Voltage",
        "Meter Power",
    ] + inverter_columns
    df = df[columns_order]

    return df


def rename(df):
    return (
        df.pipe(column_others)
        .pipe(column_temperature)
        .pipe(column_wind)
        .pipe(column_voltage)
        .pipe(column_inverter)
        .pipe(column_reorder)
    )
