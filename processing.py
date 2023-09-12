import pandas as pd
from prettytable import PrettyTable

# Used to store messages for the log file
log_messages = []

def read_data(file_name):
    lines = []

    with open(file_name, 'r') as f:
        for _ in range(10):  # Read only the first 10 lines
            line = f.readline()
            if not line:
                break
            lines.append(line)
    
    header_row = next((i for i, line in enumerate(lines) if "timestamp" in line.lower()), None)
    if header_row is None:
        raise ValueError("Header not found in the file.")

    rows_to_skip = header_row
    df = pd.read_csv(file_name,
                 skiprows=rows_to_skip, header=0)
    return df

def custom_to_datetime(s):
    try:
        return pd.to_datetime(s, format="%m/%d/%Y %I:%M:%S %p")
    except ValueError:
        return pd.to_datetime(s, format="%m/%d/%Y %H:%M:%S")
    
def change_column_names(df):
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

    # Rename the columns based on the mapping
    df.rename(columns=rename_mapping, inplace=True)

    meter_power_index = df.columns.get_loc("Meter Power")

    inverter_index = 1
    for col in df.columns[meter_power_index+1:]:
        df.rename(columns={col: 'Inverter_' + str(inverter_index)}, inplace=True)
        inverter_index += 1

    # Select only the columns that we're interested in
    required_cols = list(keyword_mapping.keys()) + [f'Inverter_{i + 1}' for i in range(inverter_index-1)]
    df = df[required_cols]

    # Add a column of 'Date'
    df['Timestamp'] = df['Timestamp'].apply(custom_to_datetime)
    df['Date'] = df['Timestamp'].dt.date  
    return df

def get_missing_info(missing_rows):
    table = PrettyTable()
    table.field_names = ["Index"] + missing_rows.columns.tolist()
    for index, row in missing_rows.iterrows():
        table.add_row([index+2] + row.tolist())
    
    return table

def log(message):
    log_messages.append(message)
    print(message)

def check_POA(df):
    missing_rows = df[df['POA Irradiance'].isna()]
    if not missing_rows.empty:
        df['POA Irradiance'] = df['POA Irradiance'].fillna(-999)
        filled_rows = df.loc[missing_rows.index]
        missing_info = get_missing_info(filled_rows)
        message = (
            f"Detected missing values in the 'POA Irradiance' column.\n"
            f"These have been filled with a placeholder value of -999.\n"
            f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
            f"Details of missing rows:\n{missing_info}"
        )
        log(message)

    else:
        message = ("All good! The 'POA Irradiance' column has no missing values.")
        log(message)

def check_and_replace_missing(df, condition_col, condition_value, col_to_check):
    condition_rows = df[df[condition_col] >= condition_value]
    missing_rows = condition_rows[condition_rows[col_to_check].isna()]
    if not missing_rows.empty:
        df.loc[missing_rows.index, col_to_check] = -999
        missing_info = get_missing_info(missing_rows)
        message = (
            f"Detected missing values in the {col_to_check} column when {condition_col} >= {condition_value}.\n"
            f"These have been filled with a placeholder value of -999.\n"
            f"There is no need to document this discrepancy in the 'Data Issues' spreadsheet.\n"
            f"Details of missing rows:\n{missing_info}"
        )
        log(message)

    else:
        message = (f"All good! The {col_to_check} column has no missing values when {condition_col} >= {condition_value}.")
        log(message)

def check_and_autofill_Meter(df):
    # Check missing values in Meter Power and auto-fill the missing value if possible
    missing_meters = df[df['Meter Power'].isna()]
    inverter_cols = [col for col in df.columns if col.startswith('Inverter_')]
    filled = []
    unfilled = []

    if not missing_meters.empty:
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
                f"{get_missing_info(filled_df)}")
            log(message)

        if not unfilled_df.empty:
            missing_by_day = unfilled_df.groupby('Date').size()
            missing_dates = missing_by_day.index.tolist()
            missing_date_str = '\n'.join([f"{date}: {count} missing" for date, count in missing_by_day.items()])
            message = (f"The missing 'Meter Power' values in the following rows cannot be auto-filled due to missing inverter values."
                f"These have been filled with a placeholder value of -999.\n"
                f"Kindly document this discrepancy in the 'Data Issues' spreadsheet for further review.\n"
                f"{get_missing_info(unfilled_df)}\n"
                f"\nSummary of missing dates:\n{missing_date_str}")
            log(message)

            return missing_dates

    else:
        log("All good! The 'Meter Power' column has no missing values.")
        return None
    

def format_workorders(workorders):
    table = PrettyTable()
    table.field_names = ["Index"] + workorders.columns.tolist()
    for index, row in workorders.iterrows():
        table.add_row([index] + row.tolist())
    return table

def fetch_work_order(work_order, missing_dates, site_name):
    work_order['Fault/Event Start - Date/Time'] = pd.to_datetime(work_order['Fault/Event Start - Date/Time'], format="%b %d, %Y %I:%M:%S %p")
    work_order['Date'] = work_order['Fault/Event Start - Date/Time'].dt.date
    fetched_records = work_order[(work_order['Date'].isin(missing_dates)) & (work_order['Site Name'].isin(["Agate"]))]
    if fetched_records.empty:
        message = (f"No work orders found for site {site_name} on the missing dates.")
        log(message)
    else:
        formatted_table = format_workorders(fetched_records)
        message = f"Information from work order:\n{formatted_table}" 
        log(message)

def extract_site_name(file_name):
    site_name = file_name.split('_')[-1].replace(" Monthly.csv", "")
    return site_name

def main():
    file_name = '2023-08-01-2023-08-31_Albertson Monthly.csv'
    site_name = extract_site_name(file_name)

    df_raw = read_data(file_name)
    df = change_column_names(df_raw)
    work_order = pd.read_csv('WorkOrdersAdministration.csv',skiprows=0, header=0)

  
    check_POA(df)
    check_and_replace_missing(df, 'POA Irradiance', 100, 'Ambient Temperature')
    check_and_replace_missing(df, 'POA Irradiance', 100, 'Wind Speed')
    missing_dates = check_and_autofill_Meter(df)
    if missing_dates:
        fetch_work_order(work_order, missing_dates,site_name)

    df.drop(columns=['Date'], inplace=True)
    df.to_csv(f'exported_{site_name}.csv', index=False)

    with open(f'log_{site_name}.txt', 'w') as file:
        for message in log_messages:
            file.write(message + '\n\n')

main()

