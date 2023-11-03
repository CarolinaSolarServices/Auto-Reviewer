import getInfo
from getInfo import log
from readData import read_site
from renameColumn import rename
from normalizeData import normalize
from checkMissing import missing
from checkWorkorder import fetch_workorder
import os


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
        # off_dates = get_off_dates(matched_records)
        # if off_dates:
        #     site_df = check_and_autofill_inverter_and_voltage(site_df, off_dates).drop(
        #         columns="Date"
        #     )
        # else:
        #     log(
        #         "No off dates found in the work order for days where missing meter power cannot be auto-filled."
        #     )
    else:
        log("No missing records to be fetched from the work order.")

    output_directory = "../output/exportedData/"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    site_df.to_csv(f"../output/exportedData/{os.path.basename(file_path)}", index=False)
    with open(f"../output/log/log_{site_name}.txt", "w") as file:
        for message in getInfo.log_messages:
            file.write(message + "\n")


def main(directory="../data"):
    for csv_file in os.scandir(directory):
        if csv_file.name.endswith("Monthly.csv"):
            print(csv_file.name)
            process_files(csv_file.path)
            getInfo.log_messages = []


if __name__ == "__main__":
    main()
