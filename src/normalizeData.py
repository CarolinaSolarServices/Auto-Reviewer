import pandas as pd
from suntime import Sun
from getGeocoding import getGeocoding
from getInfo import get_info


# Convert the datetime string to a datetime object
def custom_to_datetime(df):
    formats = [
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%y %H:%M:%S",
        "%m/%d/%y %H:%M",
        "%m-%d-%Y %H:%M:%S",
        "%m-%d-%y %H:%M:%S",
        "%m-%d-%Y %H:%M",
        "%m-%d-%y %H:%M",
    ]
    for fmt in formats:
        try:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], format=fmt)
            return df  # return DataFrame if the format matches
        except ValueError:  # if the format doesn't match, continue to the next format
            continue
    raise ValueError(
        f"No suitable format found for the 'Timestamp' column."
    )  # raise error if no suitable format is found


def determine_day_night(row, site_name):
    lat, lng = getGeocoding(site_name)
    if lat is None or lng is None:
        return "Unknown"
    sun = Sun(lat, lng)
    date = row["Timestamp"].date()
    sr = sun.get_local_sunrise_time(date).time()
    ss = sun.get_local_sunset_time(date).time()
    time = row["Timestamp"].time()
    if sr <= time <= ss:
        return "Day"
    else:
        return "Night"


def normalize(df, site_name):
    df = custom_to_datetime(df)
    df["Day/Night"] = df.apply(lambda row: determine_day_night(row, site_name), axis=1)

    return df
