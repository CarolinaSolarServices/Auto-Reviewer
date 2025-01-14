from prettytable import PrettyTable
import pandas as pd

# Create a list to store messages for the log file
log_messages = []


# Log and print messages
def log(message):
    log_messages.append(message)


# Print detailed information of selected rows with pretty format
# 'rows' is actually a df
def get_info(df):
    sub_df = get_subset(df)
    table = PrettyTable()
    table.field_names = ["Index"] + sub_df.columns.tolist()

    for index, row in sub_df.iterrows():
        table.add_row([index + 2] + row.tolist())

    return table


def get_subset(rows):
    column_count = rows.shape[1]
    if column_count > 12:
        subset = pd.concat(
            [
                rows.iloc[:, :9],
                rows.iloc[:, -3:],
            ],
            axis=1,
        )
    else:
        subset = rows

    row_count = rows.shape[0]
    if row_count > 20:
        subset = subset.iloc[:20, :]

    return subset


def format_workorder(workorder):
    table = PrettyTable()
    table.field_names = ["Index"] + workorder.columns.tolist()
    for index, row in workorder.iterrows():
        table.add_row([index] + row.tolist())
    return table
