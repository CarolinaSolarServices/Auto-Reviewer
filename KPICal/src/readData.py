import pandas as pd


def detect_separator(line):
    if "," in line:
        return ","

    return ";"


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
        print(error_message)
        exit()

    separator = detect_separator(lines[header_index])
    # The index of the found header is exactly the number of rows to skip when reading the data
    df = pd.read_csv(file_path, skiprows=header_index, header=0, sep=separator)
    # print(df.loc[0,:])

    # Check whether the second row in the dataframe is an extra unit row
    if pd.isna(df.iloc[0, 0]):
        df.drop(index=0, inplace=True)
        df.reset_index(
            drop=True, inplace=True
        )  # Resetting the index after dropping the row

    return df