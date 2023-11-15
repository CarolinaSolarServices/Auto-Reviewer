import getInfo
import renameColumn
import pandas as pd
from getInfo import log
from readData import read_site
from renameColumn import rename, name_mapping
from normalizeData import normalize
from checkMissing import missing
from checkWorkorder import fetch_workorder
import os
import shutil
from processFile import process_file
import Summary


def main(directory="../data"):
    for csv_file in os.scandir(directory):
        if csv_file.name.endswith("Monthly.csv"):
            print(csv_file.name)
            process_file(csv_file.path)
            getInfo.log_messages = []
    Summary.summary = Summary.summary.sort_values(by="Site Name")
    Summary.summary.to_csv(f"../output/summary.csv", index=False)


if __name__ == "__main__":
    main()
