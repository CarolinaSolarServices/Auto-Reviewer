import pandas as pd
from suntime import Sun
from getGeocoding import getGeocoding, getTargetTime, getTimeZone


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
        "%m/%d/%Y %H:%M",
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

        # print(f"sunrise UTC: {sr_time}, sunset UTC: {ss_time}\n")
        # Combine the date with the sunrise and sunset times
        # sr_datetime = datetime.datetime.combine(date, sr_time)
        # ss_datetime = datetime.datetime.combine(date, ss_time)
        # Convert them to Timestamps and then adjust the timezones
        # sr = getTargetTime(pd.Timestamp(sr_datetime), "UTC", tz)
        # ss = getTargetTime(pd.Timestamp(ss_datetime), "UTC", tz)
        # print(f"sunrise time: {sr}, sunset time: {ss}\n")
        # time = row["Timestamp"].time()

        # observer = ephem.Observer()
        # observer.lat, observer.lon = lat, lng
        # observer.date = row["Timestamp"].date()
        # sunrise = observer.previous_rising(ephem.Sun()).datetime()
        # sunset = observer.next_setting(ephem.Sun()).datetime()
        # print(f"Sunrise: {sunrise}, Sunset: {sunset}")
        # time = row["Timestamp"].time()
        # if sunrise <= time <= sunset:
        #     return "Day"
        # else:
        #     return "Night"


def normalize(df, site_name):
    df = custom_to_datetime(df)
    lat, lng = getGeocoding(site_name)
    tz = getTimeZone(lat, lng)
    df["Day/Night"] = df.apply(
        lambda row: determine_day_night(row, lat, lng, tz), axis=1
    )

    return df
