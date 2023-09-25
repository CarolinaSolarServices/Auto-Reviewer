def column_temperature(df):
    ambient = [
        col
        for col in df.columns
        if "ambient" in col.lower() and "temperature" in col.lower()
    ]
    if ambient:
        df.rename(columns={ambient[0]: "Temperature"}, inplace=True)
    else:
        temperature = [col for col in df.columns if "temperature" in col.lower()]
        if temperature:
            df.rename(columns={temperature[0]: "Temperature"}, inplace=True)
        else:
            df["Ambient Temperature"] = -999
    return df


def column_wind(df):
    wind = [
        col for col in df.columns if "wind" in col.lower() or "speed" in col.lower()
    ]
    if wind:
        df.rename(columns={wind[0]: "Wind Speed"}, inplace=True)
    else:
        df["Wind Speed"] = -999
    return df


def column_voltage(df):
    voltage = [col for col in df.columns if "voltage" in col.lower()]
    if len(voltage) > 1:
        less_missing = min(voltage, key=lambda col: df[col].isna().sum)
        df.rename(columns={less_missing: "Meter Voltage"}, inplace=True)
    elif voltage:
        df.rename(columns={voltage[0]: "Meter Voltage"}, inplace=True)
    else:
        df["Meter Voltage"] = -999

    return df


def column_others(df):
    keyword_mapping = {
        "Timestamp": ["timestamp"],
        "POA Irradiance": ["poa"],
        "Meter Power": ["meter", "power"],
    }

    rename_mapping = {}
    for new_name, keywords in keyword_mapping.items():
        for col in df.columns:
            if all(keyword.lower() in col.lower() for keyword in keywords):
                rename_mapping[col] = new_name
                break

    df.rename(columns=rename_mapping, inplace=True)

    return df


def column_inverter(df):
    known_columns = {
        "Timestamp",
        "POA Irradiance",
        "Meter Power",
        "Meter Voltage",
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
        "Meter Power",
        "Meter Voltage",
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
