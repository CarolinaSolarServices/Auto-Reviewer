import pandas as pd
from readData import read_workorder
from getInfo import log, format_workorder


def fetch_workorder(missing_dates, site_name):
    workorder = read_workorder()
    workorder["Fault/Event Start"] = pd.to_datetime(
        workorder["Fault/Event Start"], format="%b %d, %Y %I:%M:%S %p"
    )
    workorder["Date"] = workorder["Fault/Event Start"].dt.date
    matched_records = workorder[
        (workorder["Date"].isin(missing_dates))
        & (workorder["Site Name"].isin([site_name]))
    ]

    if matched_records.empty:
        log(f"No work orders found for site {site_name} on the missing dates.")
    else:
        info = format_workorder(matched_records.drop(columns="Date"))
        log(f"Information from work order:\n{info}")

    return matched_records


def get_off_dates(records):
    off_records = records[
        records["Description"].str.contains("offline", case=False, na=False)
    ]
    off_dates = off_records["Date"].tolist()
    return off_dates
