from prettytable import PrettyTable

# Create a list to store messages for the log file
log_messages = []


# Log and print messages
def log(message):
    log_messages.append(message)


# Print detailed information of selected rows with pretty format
def get_info(rows):
    table = PrettyTable()
    table.field_names = ["Index"] + rows.columns.tolist()
    for index, row in rows.iterrows():
        table.add_row([index + 2] + row.tolist())

    return table


def format_workorder(workorder):
    table = PrettyTable()
    table.field_names = ["Index"] + workorder.columns.tolist()
    for index, row in workorder.iterrows():
        table.add_row([index] + row.tolist())
    return table
