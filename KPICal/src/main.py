import os
from processFile import process_file
import Summary


def main(directory="../data"):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                print(f"Processing {file_path}")
                base_output_directory = "../output"
                os.makedirs(base_output_directory, exist_ok=True)
                process_file(file_path, base_output_directory)
    summary_wide = Summary.summary.pivot(
        index="Site Name", columns="Year-Month", values="Availability"
    ).reset_index()
    outdir_summary = os.path.join(base_output_directory, "summary")
    os.makedirs(outdir_summary, exist_ok=True)
    summary_wide.to_csv(os.path.join(outdir_summary, "summary.csv"), index=False)


if __name__ == "__main__":
    main()
