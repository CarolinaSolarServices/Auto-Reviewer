import os
from processFile import process_file
import Summary


def get_input_method():
    """
    Prompt the user to choose the method for inputting exclusion information.

    The user has two options:
    1. Manual input - The user will manually enter the exclusion data.
    2. Automatic reading - The program will automatically read exclusion data from a specified directory ('data/exclusion').

    The function will repeatedly prompt the user until a valid input (1 or 2) is received.

    Returns:
        int: The chosen method (1 for manual input, 2 for automatic reading).
    """

    while True:
        print("Please choose the method for inputting exclusion information:")
        print("  1. Manual input")
        print("  2. Automatic reading from 'data/exclusion'")
        user_choice = input("Enter your choice (1 or 2): ")

        if user_choice in ["1", "2"]:  # Check if the user entered a valid option
            return int(user_choice)  # Return the choice as an integer
        else:
            print(
                "Invalid input. Please enter '1' for manual input or '2' for automatic reading."
            )


def main(directory="../data"):
    # Prompt the user to choose the method for inputting exclusion information.
    input_method = get_input_method()
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv") and not file.startswith("ExclusiveOutages"):
                file_path = os.path.join(root, file)
                print(f"Processing {file_path}")
                base_output_directory = "../output"
                os.makedirs(base_output_directory, exist_ok=True)
                process_file(file_path, base_output_directory, input_method)
    summary_wide_1 = Summary.summary1.pivot(
        index="Site Name", columns="Year-Month", values="Availability"
    ).reset_index()
    summary_wide_2 = Summary.summary2.pivot(
        index="Site Name", columns="Year-Month", values="Availability"
    ).reset_index()
    outdir_summary = os.path.join(base_output_directory, "summary")
    os.makedirs(outdir_summary, exist_ok=True)
    summary_wide_1.to_csv(os.path.join(outdir_summary, "summary1.csv"), index=False)
    summary_wide_2.to_csv(os.path.join(outdir_summary, "summary2.csv"), index=False)


if __name__ == "__main__":
    main()
