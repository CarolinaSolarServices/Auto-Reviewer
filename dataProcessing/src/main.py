import getInfo
import os
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
