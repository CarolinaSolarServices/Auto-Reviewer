import pandas as pd
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

    # print(df["Timestamp"].head())

    for fmt in formats:
        try:
            # print(f"Try to use {fmt}")
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
    cols_to_convert = df.columns[df.columns != "Timestamp"]
    df[cols_to_convert] = df[cols_to_convert].apply(pd.to_numeric, errors="coerce")
    df = custom_to_datetime(df)
    # print("Successfully converted.")

    lat, lng = getGeocoding(site_name)
    tz = getTimeZone(lat, lng)
    new_columns = df.apply(lambda row: determine_day_night(row, lat, lng, tz), axis=1)
    new_columns_df = pd.DataFrame(new_columns, columns=["Day/Night"])

    df = pd.concat([df, new_columns_df], axis=1)

    return df
