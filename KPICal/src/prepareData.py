import os
import pandas as pd
from openpyxl.utils.exceptions import InvalidFileException
from zipfile import BadZipFile


"""
    Convert Excel files (.xlsx) in a specified directory to CSV format.
    
    This function scans through all the Excel files in the given input directory,
    converts each sheet (except for 'Sheet 1') into a CSV file, and saves the
    CSV files into a year-month based directory structure under the given base output directory.
"""


def xlsx_to_csv(input_directory, base_output_directory):
    os.makedirs(base_output_directory, exist_ok=True)
    for file in os.scandir(input_directory):
        try:
            xls = pd.ExcelFile(file.path)
            for sheet_name in xls.sheet_names:
                if sheet_name != "Sheet 1":
                    df = pd.read_excel(file.path, sheet_name=sheet_name)
                    year_date = file.name.split("_")[0][:7]
                    output_directory = os.path.join(base_output_directory, year_date)
                    os.makedirs(output_directory, exist_ok=True)

                    csv_file_name = f"{year_date}_{sheet_name}.csv"
                    csv_file_path = os.path.join(output_directory, csv_file_name)
                    df.to_csv(csv_file_path, index=False)
                    print(f"Converted {file.name} sheet {sheet_name} to CSV format.")

        except BadZipFile as e:
            print(f"Could not process {file.name}: File is not a valid zip file.")
        except InvalidFileException as e:
            print(f"Could not process {file.name}: File is not a valid Excel file.")
        except Exception as e:
            print(f"An unexpected error occurred while processing {file.name}: {e}")
