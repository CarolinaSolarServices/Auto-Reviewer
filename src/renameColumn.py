import numpy as np


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
    # ambient = [
    #     col
    #     for col in df.columns
    #     if "ambient" in col.lower() and "temperature" in col.lower()
    # ]
    # if ambient:
    #     df.rename(columns={ambient[0]: "Temperature"}, inplace=True)
    # else:
    #     temperature = [col for col in df.columns if "temperature" in col.lower()]
    #     if temperature:
    #         df.rename(columns={temperature[0]: "Temperature"}, inplace=True)
    #     else:
    #         df["Ambient Temperature"] = -999
    return df


def column_wind(df):
    wind = [
        col for col in df.columns if "wind" in col.lower() or "speed" in col.lower()
    ]
    if wind:
        df.rename(columns={wind[0]: "Wind Speed"}, inplace=True)
    else:
        df["Wind Speed"] = np.nan
    return df


def column_voltage(df):
    voltage = [col for col in df.columns if "voltage" in col.lower()]
    if len(voltage) > 1:
        less_missing = min(voltage, key=lambda col: df[col].isna().sum())
        df.rename(columns={less_missing: "Meter Voltage"}, inplace=True)
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
            df.rename(columns={col: "Inverter_" + str(inverter_index)}, inplace=True)
            inverter_index += 1

    return df


def column_reorder(df):
    inverter_columns = sorted(col for col in df.columns if "Inverter" in col)
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
    # df = column_others(df)
    # df = column_temperature(df)
    # df = column_wind(df)
    # df = column_voltage(df)
    # df = column_inverter(df)
    # df = column_reorder(df)
    # print(df.columns)

    # return df
    return (
        df.pipe(column_others)
        .pipe(column_temperature)
        .pipe(column_wind)
        .pipe(column_voltage)
        .pipe(column_inverter)
        .pipe(column_reorder)
    )
