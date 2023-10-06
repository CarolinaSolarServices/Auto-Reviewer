import pandas as pd
from readData import read_workorder
from getGeocoding import getGeocoding, getTimeZone, getTargetTime
from getInfo import log, format_workorder


def is_any_date_in_range(start, end, mark, missing_dates):
    if pd.notna(end):
        date_range = pd.date_range(start, end).date.tolist()
        # print(date_range)
    elif pd.notna(mark):
        date_range = pd.date_range(start, mark).date.tolist()
        # print(date_range)
    else:
        date_range = [start.date()]
        # print(date_range)

    return any(date in missing_dates for date in date_range)


def convert_time(time_str, local_timezone, target_timezone):
    dt = pd.to_datetime(time_str, format="%b %d, %Y %I:%M:%S %p")
    # print(f"dt original: {dt}")
    dt = getTargetTime(dt, local_timezone, target_timezone)
    # print(f"dt converted: {dt}")

    return dt


def fetch_workorder(missing_dates, site_name):
    workorder = read_workorder()
    site_workorder = workorder[workorder["Site Name"].isin([site_name])].copy()

    lat, lng = getGeocoding(site_name)
    local_timezone = "America/New_York"
    target_timezone = getTimeZone(lat, lng)

    site_workorder["Fault/Event Start"] = site_workorder["Fault/Event Start"].apply(
        convert_time, args=(local_timezone, target_timezone)
    )
    site_workorder["Fault End"] = site_workorder["Fault End"].apply(
        convert_time, args=(local_timezone, target_timezone)
    )
    site_workorder["Date When Marked Complete/Incomplete"] = site_workorder[
        "Date When Marked Complete/Incomplete"
    ].apply(convert_time, args=(local_timezone, target_timezone))

    # site_workorder["Fault/Event Start"] = pd.to_datetime(
    #     site_workorder["Fault/Event Start"], format="%b %d, %Y %I:%M:%S %p"
    # )
    # site_workorder["Fault End"] = pd.to_datetime(
    #     site_workorder["Fault End"], format="%b %d, %Y %I:%M:%S %p"
    # )
    # site_workorder["Date When Marked Complete/Incomplete"] = pd.to_datetime(
    #     site_workorder["Date When Marked Complete/Incomplete"], format="%b %d, %Y %I:%M:%S %p"
    # )

    # Converted local time to EST time
    # site_workorder["Fault/Event Start EST"] = site_workorder["Fault/Event Start"].apply(
    #     getEST, args=(local_timezone, target_timezone)
    # )
    # site_workorder["Fault End"] = site_workorder["Fault End"].apply(
    #     getEST, args=(local_timezone, target_timezone)
    # )
    # site_workorder["Date When Marked Complete/Incomplete"] = site_workorder["Date When Marked Complete/Incomplete"].apply(
    #     getEST, args=(local_timezone, target_timezone)
    # )

    # print(site_workorder.iloc[0]["Fault/Event Start"])
    # print(site_workorder.iloc[0]["Fault End"])
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


# def get_off_dates(records):
#     off_records = records[
#         records["Description"].str.contains("offline", case=False, na=False)
#     ]
#     off_dates = off_records["Date"].tolist()
#     return off_dates
