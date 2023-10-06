import pandas as pd
from getInfo import log_messages, log
from readData import read_site
from renameColumn import rename
from normalizeData import normalize
from checkMissing import missing
from checkWorkorder import fetch_workorder

file_name = "2023-09-01-2023-09-30_Woodline Monthly.csv"
file_path = "../data/" + file_name
site_name = file_path.split("_")[-1].replace(" Monthly.csv", "")


def main():
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

    site_df.to_csv(f"../output/exportedData/{file_name}", index=False)
    with open(f"../output/log/log_{site_name}.txt", "w") as file:
        for message in log_messages:
            file.write(message + "\n")


if __name__ == "__main__":
    main()
