import pandas as pd
from timezonefinder import TimezoneFinder


def getGeocoding(site_name):
    df = pd.read_csv("../data/geoCoding.csv")
    row = df.loc[df["Site Name"] == site_name]
    if row.empty:
        return None, None
    return row.iloc[0]["Latitude"], row.iloc[0]["Longitude"]


def getTimeZone(latitude, longitude):
    tf = TimezoneFinder()
    return tf.timezone_at(lng=longitude, lat=latitude)


def getEST(original_time, local_timezone, target_timezone):
    local_time = original_time.tz_localize(local_timezone)
    target_time = local_time.tz_convert(target_timezone)

    return target_time
