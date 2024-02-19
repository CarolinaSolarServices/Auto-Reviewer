import pandas as pd
import numpy as np
import os
from calculateExpected import process_inverter, compute_avg


def validate_datetime_input(datetime_str, time_point):
    """
    Validates and returns the datetime object for a given date or datetime string.
    """
    date_time_formats = [
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

    date_only_formats = [
        "%Y-%m-%d",  # Date with century and hyphen separator (e.g., 2023-06-30)
        "%m/%d/%Y",  # U.S. date format with century and slash separator (e.g., 06/30/2023)
        "%m-%d-%Y",  # U.S. date format with century and hyphen separator (e.g., 06-30-2023)
        "%d/%m/%Y",  # Rest-of-world date format with century and slash separator (e.g., 30/06/2023)
        "%d-%m-%Y",  # Rest-of-world date format with century and hyphen separator (e.g., 30-06-2023)
        "%Y/%m/%d",  # ISO-like date format with century and slash separator (e.g., 2023/06/30)
        "%Y%m%d",  # Compact date format with century (e.g., 20230630)
        "%m/%d/%y",  # U.S. date format without century and slash separator (e.g., 06/30/23)
        "%m-%d-%y",  # U.S. date format without century and hyphen separator (e.g., 06-30-23)
        "%d/%m/%y",  # Rest-of-world date format without century and slash separator (e.g., 30/06/23)
        "%d-%m-%y",  # Rest-of-world date format without century and hyphen separator (e.g., 30-06-23)
        "%y/%m/%d",  # ISO-like date format without century and slash separator (e.g., 23/06/30)
        "%y%m%d",  # Compact date format without century (e.g., 230630)
        "%b %d, %Y",  # Date with textual month, day and century (e.g., Jun 30, 2023)
        "%d %b %Y",  # Date with day, textual month and century (e.g., 30 Jun 2023)
        "%b %d, %y",  # Date with textual month, day without century (e.g., Jun 30, 23)
        "%d %b %y",  # Date with day, textual month without century (e.g., 30 Jun 23)
        "%B %d, %Y",  # Date with full textual month, day and century (e.g., June 30, 2023)
        "%d %B %Y",  # Date with day, full textual month and century (e.g., 30 June 2023)
        "%B %d, %y",  # Date with full textual month, day without century (e.g., June 30, 23)
        "%d %B %y",  # Date with day, full textual month without century (e.g., 30 June 23)
    ]

    # Try parsing the string using date only formats
    for fmt in date_only_formats:
        try:
            datetime_obj = pd.to_datetime(datetime_str, format=fmt, errors="raise")
            if time_point == "end":
                # Adjust to the end of the day for "end" time points
                datetime_obj += pd.Timedelta(days=1)
            return datetime_obj
        except ValueError:
            continue

    # If not successful, try parsing using date-time formats
    for fmt in date_time_formats:
        try:
            datetime_obj = pd.to_datetime(datetime_str, format=fmt, errors="raise")
            return datetime_obj
        except ValueError:
            continue

    raise ValueError(f"Can't parse the {time_point} datetime.")


def prompt_for_outages(site_name):
    outages = []
    outage_count = 0

    while True:
        print(
            f"\nPlease enter information for {site_name}'s exclusive outage #{outage_count + 1}. Enter '0' to stop."
        )
        start = input(
            "Enter the start date (YYYY-MM-DD) or date and time (YYYY-MM-DD HH:MM): "
        )
        if start == "0":
            break

        end = input(
            "Enter the end date (YYYY-MM-DD) or date and time (YYYY-MM-DD HH:MM): "
        )
        if end == "0":
            break

        inverter_id = input("Enter the inverter ID (e.g., 1, 2, 3): ")
        if inverter_id == "0":
            break

        try:
            # Validate start datetime
            start_datetime = validate_datetime_input(start, "start")

            # Validate end datetime
            end_datetime = validate_datetime_input(end, "end")

            # Validate that end is after start
            if end_datetime <= start_datetime:
                raise ValueError("End date/time must be after the start date/time.")

            # Validate inverter ID
            inverter_num = int(inverter_id)
            if inverter_num < 1:
                raise ValueError("Inverter ID must be a positive integer.")

        except ValueError as e:
            print(f"Invalid input: {e} Please try again!")
            continue  # If any validation fails, restart the loop for this outage

        # Add validated outage information to the list
        outages.append((start_datetime, end_datetime, inverter_num))
        outage_count += 1

    return outages


def read_outages_from_csv(site_name):
    filename = f"ExclusiveOutages_{site_name}.csv"
    file_dir = os.path.join("../data/exclusions", filename)
    try:
        df = pd.read_csv(
            file_dir,
        )
    except FileNotFoundError:
        raise ValueError(f"File {filename} not found.")

    outages = []
    for index, row in df.iterrows():
        try:
            start = row.iloc[0]  # The first column is start_time
            end = row.iloc[1]  # The second column is end_time
            inverter_id = row.iloc[2]  # The third column is inverter_id

            # Validate start datetime
            start_datetime = validate_datetime_input(start, "start")

            # Validate end datetime
            end_datetime = validate_datetime_input(end, "end")

            # Validate that end is after start
            if end_datetime <= start_datetime:
                raise ValueError("End date/time must be after the start date/time.")

            # Validate inverter ID
            inverter_num = int(inverter_id)
            if inverter_num < 1:
                raise ValueError("Inverter ID must be a positive integer.")

            outages.append((start_datetime, end_datetime, inverter_num))

        except ValueError as e:
            print(f"{site_name} @ Row {index + 1} : Invalid input - {e}")
            continue

    return outages


def mark_exclusive_outages(df, outages):
    """
    Fill cells of exclusive outages with "Exclusive Outage".

    Parameters:
    - df: Original DataFrame
    - outages: List of tuples, each tuple contains start time, end time, and inverter ID.

    Returns:
    - DataFrame copy with specific cells filled with NaN.
    """

    df_copy = df.copy(deep=True)

    for start, end, inverter_id in outages:
        start_time = pd.to_datetime(start)
        end_time = pd.to_datetime(end)
        inverter_col = f"Inverter_{inverter_id}"

        # Fill the cells with NaN only if the existing value is not greater than 0
        mask = (df_copy["Timestamp"] >= start_time) & (df_copy["Timestamp"] < end_time)
        df_copy.loc[mask, inverter_col] = df_copy.loc[mask, inverter_col].apply(
            lambda x: x if x > 0 else np.nan
        )

    return df_copy


def calculate_with_exclusions(df, site_name, input_method):
    if input_method == 1:
        outages = prompt_for_outages(site_name)
    else:
        outages = read_outages_from_csv(site_name)

    df_without_exclusions = mark_exclusive_outages(df, outages)
    return process_inverter(df_without_exclusions, compute_avg(df))
