import pandas as pd
from suntime import Sun
from getGeocoding import getGeocoding


# Convert the datetime string to a datetime object
def custom_to_datetime(df):
    formats = [
        "%m/%d/%Y %I:%M:%S %p",  # MM/DD/YYYY HH:MM:SS AM/PM
        "%m/%d/%Y %H:%M:%S",  # MM/DD/YYYY 24-hour
        "%Y-%m-%d %H:%M:%S",  # YYYY-MM-DD 24-hour
        "%d/%m/%Y %H:%M:%S",  # DD/MM/YYYY 24-hour
        # Feel free to add more formats as needed
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
