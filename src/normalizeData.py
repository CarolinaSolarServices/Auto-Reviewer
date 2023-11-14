import pandas as pd
import numpy as np
from suntime import Sun
from getGeocoding import getGeocoding, getTargetTime, getTimeZone


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


def determine_day_night(row, lat, lng, tz):
    if lat is None or lng is None or tz is None:
        return "Unknown"
    else:
        sun = Sun(lat, lng)
        date = row["Timestamp"].date()
        sr = sun.get_sunrise_time(date)
        ss = sun.get_sunset_time(date)
        sr_local = getTargetTime(pd.Timestamp(sr), "UTC", tz)
        ss_local = getTargetTime(pd.Timestamp(ss), "UTC", tz)
        if sr_local.time() <= row["Timestamp"].time() <= ss_local.time():
            return "Day"
        else:
            return "Night"


def normalize(df, site_name):
    cols_to_convert = df.columns[df.columns != "Timestamp"]
    df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors="coerce")
    df = custom_to_datetime(df)
    lat, lng = getGeocoding(site_name)
    tz = getTimeZone(lat, lng)
    new_columns = df.apply(lambda row: determine_day_night(row, lat, lng, tz), axis=1)
    new_columns_df = pd.DataFrame(new_columns, columns=["Day/Night"])

    df = pd.concat([df, new_columns_df], axis=1)

    return df
