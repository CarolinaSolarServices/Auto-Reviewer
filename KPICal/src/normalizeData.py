import pandas as pd


# Convert the datetime string to a datetime object
def custom_to_datetime(df):
    formats = [
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%m/%d/%Y %I:%M:%S %p",
        "%m-%d-%Y %H:%M:%S",
        "%m-%d-%y %H:%M:%S",
        "%m-%d-%Y %H:%M",
        "%m-%d-%y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d %H:%M",
    ]

    for fmt in formats:
        try:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], format=fmt)
            return df

        except ValueError:  # if the format doesn't match, continue to the next format
            continue

    # Quit the program if no suitable format is found
    raise ValueError("No suitable format found for the 'Timestamp' column.")


def normalize(df):
    cols_to_convert = df.columns[df.columns != "Timestamp"]
    # Convert all values in columns except for Timestamp to numbers
    df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors="coerce")
    df = custom_to_datetime(df)

    return df
