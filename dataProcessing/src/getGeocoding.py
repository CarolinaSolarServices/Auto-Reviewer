import pandas as pd
from timezonefinder import TimezoneFinder


def getGeocoding(site_name):
    df = pd.read_csv("../data/geoCoding.csv")
    row = df.loc[df["Site Name"] == site_name]
    if row.empty:
        return None, None
    return row.iloc[0]["Latitude"], row.iloc[0]["Longitude"]


def getTimeZone(latitude, longitude):
    if latitude and longitude:
        tf = TimezoneFinder()
        return tf.timezone_at(lng=longitude, lat=latitude)

    return None


def getTargetTime(original_time, original_timezone, target_timezone):
    if original_time.tzinfo is None:
        original_time = original_time.tz_localize(original_timezone)
    target_time = original_time.tz_convert(target_timezone)

    return target_time
