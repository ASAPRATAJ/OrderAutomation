---
# Data Fetching and Spreadsheet Update Script

This script fetches data from a MySQL database and updates a Google Spreadsheet accordingly.

## Overview

The project consists of three main files:

- **Main.py**: The main script for launching the data fetching process and updating the Google Spreadsheet.
- **fetch_from_db.py**: Script for fetching data from a MySQL database.
- **push_to_excel.py**: Script for updating data in Google Spreadsheets.

## Setup

Before running the script, ensure you have installed the required Python libraries listed in `requirements.txt`. 
You can install them using the following command: `pip install -r requirements.txt`


## Configuration

Make sure to update the following configurations:

- **MySQL Database Credentials**: Update the `username`, `password`, `host`, and `database` fields in `Main.py` and `fetch_from_db.py` with your MySQL database credentials.
- **Google Spreadsheet Credentials**: Ensure you have a `credentials.json` file containing your Google service account key, and update the `key_file_path` variable in `push_to_excel.py` accordingly. Also, update the `SPREADSHEET_ID` and `RANGE_NAME` variables with your Google Spreadsheet ID and range name.

## Usage

To run the script, execute `main.py`. This script fetches data from the MySQL database and updates the Google Spreadsheet with the latest information.

## Functionality

- **Fetching Data**: The script retrieves various order details from the MySQL database, including product names, quantities, delivery dates, shipping addresses, comments, and more.
- **Updating Spreadsheet**: It updates the Google Spreadsheet with the fetched data, ensuring accurate and up-to-date information.
- **Sorting Orders**: The script sorts orders in the Google Spreadsheet based on delivery dates and removes rows with empty date strings.
- **Adding New Orders**: It checks for missing orders in the spreadsheet and adds them accordingly.
- **Order Attributes**: The script retrieves additional order attributes such as topper, candles, and other decorations, and updates the spreadsheet with this information.

## Future Improvements

- **Test Cases**: Implement test cases to ensure script reliability and robustness.
- **Credential Handling**: Improve the way of passing credentials to the database in the script, such as hashing or using environment variables.

Feel free to enhance this README further as your project evolves or if you have specific instructions or usage details to add.

