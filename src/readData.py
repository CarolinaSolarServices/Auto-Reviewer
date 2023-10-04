import pandas as pd
from getInfo import log


#  Read in the data file
def read_site(file_path):
    lines = []

    # Read only the first 10 lines into the 'lines' list
    with open(file_path, "r") as f:
        for _ in range(10):
            line = f.readline()
            if not line:
                break
            lines.append(line)

    # The first line where the word "timpestamps" (in any case) appears is the header line
    header_index = next(
        (i for i, line in enumerate(lines) if "timestamp" in line.lower()), None
    )

    if header_index is None:
        error_message = "Header not found in the file."
        log(error_message)
        raise ValueError("Header not found in the file.")

    # The index of the found header is exactly the number of rows to skip when reading the data
    df = pd.read_csv(file_path, skiprows=header_index, header=0)
    return df


def read_workorder():
    workorder = pd.read_csv("../data/WorkOrder.csv", skiprows=0, header=0)
    return workorder