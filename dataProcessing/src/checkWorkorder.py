import pandas as pd
from readData import read_workorder
from getGeocoding import getGeocoding, getTimeZone, getTargetTime
from getInfo import log, format_workorder

workorder = read_workorder()


def is_any_date_in_range(start, end, mark, missing_dates):
    if pd.notna(end):
        date_range = pd.date_range(start, end).date.tolist()

    elif pd.notna(mark):
        date_range = pd.date_range(start, mark).date.tolist()

    elif pd.isna(start):
        return False

    else:
        date_range = [start.date()]

    return any(date in missing_dates for date in date_range)


def convert_time(time_str, local_timezone, target_timezone):
    dt = pd.to_datetime(time_str, format="%b %d, %Y %I:%M:%S %p")
    dt = getTargetTime(dt, local_timezone, target_timezone)

    return dt


def fetch_workorder(missing_dates, site_name):
    site_workorder = workorder[workorder["Site Name"] == site_name].copy()
    lat, lng = getGeocoding(site_name)
    local_timezone = "America/New_York"
    target_timezone = getTimeZone(lat, lng)
    cols_to_convert = [
        "Fault/Event Start",
        "Fault End",
        "Date When Marked Complete/Incomplete",
    ]
    for col in cols_to_convert:
        # custom_to_datetime(site_workorder, col)
        site_workorder[col] = site_workorder[col].apply(
            convert_time, args=(local_timezone, target_timezone)
        )
    site_workorder["Related"] = site_workorder.apply(
        lambda row: is_any_date_in_range(
            row["Fault/Event Start"],
            row["Fault End"],
            row["Date When Marked Complete/Incomplete"],
            missing_dates,
        ),
        axis=1,
    )

    matched_records = site_workorder[site_workorder["Related"]]

    if matched_records.empty:
        log(f"No work orders found for site {site_name} on the missing dates.")
    else:
        info = format_workorder(matched_records.drop(columns="Related"))
        log(
            f"You may find the following records from the work order to be of assistance:\n{info}"
        )

    return matched_records
