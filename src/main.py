import getInfo
import renameColumn
from getInfo import log
from readData import read_site
from renameColumn import rename, name_mapping
from normalizeData import normalize
from checkMissing import missing
from checkWorkorder import fetch_workorder
import os
import shutil


def revert_inverter_names(df, name_mapping):
    df.rename(columns=name_mapping, inplace=True)
    return df


def process_files(file_path):
    site_name = file_path.split("_")[-1].replace(" Monthly.csv", "")

    sitedata = read_site(file_path)
    site_df = sitedata.pipe(rename).pipe(normalize, site_name)

    # Check and autofill columns of 'irradiance,' 'temperature,' 'wind speed,' and 'meter power',
    # and return the missing dates where missing meter power cannot be auto-filled.
    missing_dates = missing(site_df)

    log("\nV.\n")
    if missing_dates:
        fetch_workorder(missing_dates, site_name)
    else:
        log("No missing records to be fetched from the work order.")

    site_df = revert_inverter_names(site_df, name_mapping)
    renameColumn.name_mapping = {}

    output_directory = "../output/exportedData/"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    site_df.to_csv(f"../output/exportedData/{os.path.basename(file_path)}", index=False)
    with open(f"../output/log/log_{site_name}.txt", "w") as file:
        for message in getInfo.log_messages:
            file.write(message + "\n")

    # processed_directory = "../data/processed"
    # if not os.path.exists(processed_directory):
    #     os.makedirs(processed_directory)

    # shutil.move(file_path, f"{processed_directory}/{os.path.basename(file_path)}")


def main(directory="../data"):
    for csv_file in os.scandir(directory):
        if csv_file.name.endswith("Monthly.csv"):
            print(csv_file.name)
            process_files(csv_file.path)
            getInfo.log_messages = []


if __name__ == "__main__":
    main()
