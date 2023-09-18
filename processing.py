import pandas as pd
from prettytable import PrettyTable
from suntime import Sun

# Use the approximate location info of Chapel Hill to retrive sun times for now
# Will use more precise latitude and longitude data for each site in the future
longitude = -79.05
latitude = 35.91

# Used to store messages for the log file
log_messages = []

# Log and print messages
def log(message):
    log_messages.append(message)

# Print detailed information of selected rows with pretty format
def get_info(rows):
    table = PrettyTable()
    table.field_names = ["Index"] + rows.columns.tolist()
    for index, row in rows.iterrows():
        table.add_row([index+2] + row.tolist())
    
    return table


#########################################################################


# Read in the data file
def read_data(file_name):
    lines = []

    # Read only the first 10 lines into the 'lines' list
    with open(file_name, 'r') as f:
        for _ in range(10):  
            line = f.readline()
            if not line:
                break
            lines.append(line)
    
    # Assume that the first line where the word "timpestamps" (in any case) appears is the header line
    header_index = next((i for i, line in enumerate(lines) if "timestamp" in line.lower()), None)

    if header_index is None:
        error_message = "Header not found in the file."
        log(error_message)
        raise ValueError("Header not found in the file.")

    # The index of the found header is exactly the number of rows to skip when reading the data
    df = pd.read_csv(file_name,
                 skiprows=header_index, header=0)
    return df


# Convert the datetime string to a datetime object
def custom_to_datetime(s):
    formats = [
        "%m/%d/%Y %I:%M:%S %p",  # MM/DD/YYYY HH:MM:SS AM/PM
        "%m/%d/%Y %H:%M:%S",     # MM/DD/YYYY 24-hour 
        "%Y-%m-%d %H:%M:%S",     # YYYY-MM-DD 24-hour 
        "%d/%m/%Y %H:%M:%S",     # DD/MM/YYYY 24-hour 
        
        # Feel free to add more formats as needed
    ]

    for fmt in formats:
        try:
            return pd.to_datetime(s, format = fmt)
        except ValueError:
            continue
    
    # If none of the formats match, raise an error.
    raise ValueError(f"Time '{s}' is not in the expected format.")


# Determine wheter the missing value is from daytime or nighttime.
def determine_day_night(row, longitude, latitude):
    sun = Sun(latitude, longitude)
    date = row['Timestamp'].date()
    sr = sun.get_local_sunrise_time(date).time()
    ss = sun.get_local_sunset_time(date).time()
    time = row['Timestamp'].time()
    if sr <= time <= ss:
        return 'Day'
    else:
        return 'Night'
    

# Standardize the column names
def change_column_names(df):
    # Define a mapping of standardized column names to their potential keywords
    # Feel free to modify the keyword pattern as needed
    keyword_mapping = {
        'Timestamp': ['timestamp'],
        'POA Irradiance': ['poa'],
        'Ambient Temperature': ['temperature', 'ambient'],
        'Wind Speed': ['wind speed'],
        'Meter Voltage': ['voltage'],
        'Meter Power': ['meter', 'power']
    }
 
    rename_mapping = {}
    for new_name, keywords in keyword_mapping.items():
        for col in df.columns:
            if all(keyword.lower() in col.lower() for keyword in keywords):
                rename_mapping[col] = new_name
                break

    # Rename columns excluding inverters according to the rename_mapping dictionary
    df.rename(columns=rename_mapping, inplace=True)

    # Rename inverter columns 
    meter_power_index = df.columns.get_loc("Meter Power")
    inverter_index = 1
    for col in df.columns[meter_power_index+1:]:
        df.rename(columns={col: 'Inverter_' + str(inverter_index)}, inplace=True)
        inverter_index += 1

    # Select only the columns that we're interested in
    renamed_cols = list(rename_mapping.values()) + [f'Inverter_{i + 1}' for i in range(inverter_index-1)]
    df = df[renamed_cols]

    return df

def modify_columns(df, longitude, latitude):
    # Convert the timestamps into datetime objects
    df['Timestamp'] = df['Timestamp'].apply(custom_to_datetime)
    # Add a 'Day/Night' column
    df['Day/Night'] = df.apply(lambda row: determine_day_night(row, longitude, latitude), axis = 1)

    return df

#########################################################################


# Check if there is any missing value within the Irradiance column
def check_missing_irradiance(df):
    missing_rows = df[df['POA Irradiance'].isna()]
    if not missing_rows.empty:
        df['POA Irradiance'].fillna(-999, inplace=True)
        missing_rows = df.loc[missing_rows.index]

        info = get_info(missing_rows)
        message = (
            f"Detected missing values in the 'POA Irradiance' column.\n"
            f"These have been filled with a placeholder value of -999.\n"
            f"Details of all missing rows:\n{info}"
        )
        log(message)
        
        missing_during_day = missing_rows[missing_rows['Day/Night'] == 'Day']
        if not missing_during_day.empty:
            info_day = get_info(missing_during_day)
            message = (
                f"\nHere are missing values during the daytime.\n"
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"\n{info_day}"
            )
            log(message)
        
        else:
            message = (f"It seems that all the missing values occur during nighttime hours.\n"
            f"These nighttime omissions are not critical and can be safely overlooked for most analyses."
            f"Therefore, there is no need to document this discrepancy in the 'Data Issues' spreadsheet.\n")
            log(message)

    else:
        message = ("All good! The 'POA Irradiance' column has no missing values.")
        log(message)



#########################################################################


# Helper function for processing temperature and wind speed columns
# condition_value = 100
def check_and_replace_missing(df, condition_value, col_to_check):
    condition_rows = df[df['POA Irradiance'] >= condition_value]
    missing_rows = condition_rows[condition_rows[col_to_check].isna()]
    if not missing_rows.empty:
        df.loc[missing_rows.index, col_to_check] = -999
        missing_info = get_info(missing_rows)
        message = (
            f"Detected missing values in the {col_to_check} column when {'POA Irradiance'} >= {condition_value}.\n"
            f"These have been filled with a placeholder value of -999.\n"
            f"There is no need to document this discrepancy in the 'Data Issues' spreadsheet.\n"
            f"Details of missing rows:\n{missing_info}"
        )
        log(message)

    else:
        message = (f"All good! When {'POA Irradiance'} >= {condition_value}, the {col_to_check} column has no missing values .\n")
        log(message)


#########################################################################

# Check missing values in Meter Power and auto-fill the missing value if possible
def check_and_autofill_Meter(df):
    missing_meters = df[(df['Meter Power'].isna()) & (df['Day/Night'] == 'Day')]
    inverter_cols = [col for col in df.columns if col.startswith('Inverter_')]
    filled = []
    unfilled = []

    if not missing_meters.empty:
        message = f"Detected missing 'Meter Power' values during daytime."
        log(message)
        for index, row in missing_meters.iterrows():
            if not row[inverter_cols].isna().any():
                df.loc[index, 'Meter Power'] = row[inverter_cols].sum()
                updated_row = df.loc[index] 
                filled.append(updated_row)
            else:
                df.loc[index, 'Meter Power'] = -999
                updated_row = df.loc[index] 
                unfilled.append(updated_row)
        
                
        filled_df = pd.DataFrame(filled)
        unfilled_df = pd.DataFrame(unfilled)

        if not filled_df.empty:
            message = (f"The missing 'Meter Power' values in the following rows have been auto-filled based on the sum of inverter values.\n"
                f"{get_info(filled_df)}")
            log(message)

        if not unfilled_df.empty:
            info = get_info(unfilled_df)
            unfilled_df['Date'] = unfilled_df['Timestamp'].dt.date
            missing_by_day = unfilled_df.groupby('Date').size()
            missing_dates = missing_by_day.index.tolist()
            missing_date_str = '\n'.join([f"{date}: {count} missing" for date, count in missing_by_day.items()])
            message = (f"The missing 'Meter Power' values in the following rows cannot be auto-filled due to missing inverter values.\n"
                f"These have been filled with a placeholder value of -999.\n"
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"{info}\n"
                f"\nSummary of missing dates:\n{missing_date_str}\n")
            log(message)

            return missing_dates

    else:
        log("All good! The 'Meter Power' column has no missing values during daytime.")
        return None


#########################################################################


def format_workorders(workorders):
    table = PrettyTable()
    table.field_names = ["Index"] + workorders.columns.tolist()
    for index, row in workorders.iterrows():
        table.add_row([index] + row.tolist())
    return table

def fetch_work_order(work_order, missing_dates, site_name):
    work_order['Fault/Event Start'] = pd.to_datetime(work_order['Fault/Event Start'], format="%b %d, %Y %I:%M:%S %p")
    work_order['Date'] = work_order['Fault/Event Start'].dt.date
    fetched_records = work_order[(work_order['Date'].isin(missing_dates)) & (work_order['Site Name'].isin([site_name]))]
    if fetched_records.empty:
        log(f"No work orders found for site {site_name} on the missing dates.")
    else:
        formatted_table = format_workorders(fetched_records)
        log(f"Information from work order:\n{formatted_table}" )

    return fetched_records


def off_dates(records):
    off_records = records['Description'].str.contains('offline', case=False)


#########################################################################


def extract_site_name(file_name):
    site_name = file_name.split('_')[-1].replace(" Monthly.csv", "")
    return site_name


def main():
    file_name = '2023-08-01-2023-08-31_Agate Bay Monthly.csv'
    site_name = extract_site_name(file_name)

    df_raw = read_data(file_name)
    df = change_column_names(df_raw)
    df = modify_columns(df, longitude, latitude)
    work_order = pd.read_csv('(PE)OpenedWOs.csv',skiprows=0, header=0)

    log("\nI.\n")
    check_missing_irradiance(df)
    
    log("\nII.\n")
    if 'Ambient Temperature' in df.columns:
        check_and_replace_missing(df, 100, 'Ambient Temperature')
    if 'Wind Speed' in df.columns:
        check_and_replace_missing(df, 100, 'Wind Speed')
    
    log("\nIII.\n")
    missing_dates = check_and_autofill_Meter(df)
    
    log("\nIV.\n")
    if missing_dates:
        ferched_records = fetch_work_order(work_order, missing_dates, site_name)
    else:
        log("None")

    df.to_csv(f'exported_{site_name}.csv', index=False)

    with open(f'log_{site_name}.txt', 'w') as file:
        for message in log_messages:
            file.write(message + '\n\n')

main()

