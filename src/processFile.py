import getInfo
import renameColumn
import pandas as pd
from getInfo import log
from readData import read_site
from renameColumn import rename
from normalizeData import normalize
from checkMissing import missing
from checkWorkorder import fetch_workorder
import os
import shutil
import Summary


def process_file(file_path):
    log(
        f"\nNotes:\n"
        f"If the record count exceeds 20, only the first 20 rows will be displayed in this log.\n"
        f"If the number of inverter columns exceeds 12, only the first 9 and the last 3 columns will be displayed.\n"
        f"The inverter names are standardized in this log according to the order of inverter columns in the original dataset.\n"
    )
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

    site_df.rename(columns=renameColumn.name_mapping, inplace=True)
    renameColumn.name_mapping = {}

    output_directory = "../output/exportedData/"
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    site_df.to_csv(f"../output/exportedData/{os.path.basename(file_path)}", index=False)
    with open(f"../output/log/log_{site_name}.txt", "w") as file:
        for message in getInfo.log_messages:
            file.write(message + "\n")

    processed_directory = "../data/processed"
    if not os.path.exists(processed_directory):
        os.makedirs(processed_directory)

    shutil.move(file_path, f"{processed_directory}/{os.path.basename(file_path)}")

    new_row = {
        "Site Name": site_name,
        "Production": Summary.production_status,
        "Irradiance": Summary.irradiance_status,
        "Inverter": Summary.inverter_status,
    }

    Summary.summary = pd.concat(
        [Summary.summary, pd.DataFrame([new_row])], ignore_index=True
    )
    Summary.production_status = ""
    Summary.irradiance_status = ""
    Summary.inverter_status = ""
