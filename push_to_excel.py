"""Script for updating data in google spreadsheets."""
import gspread
from google.oauth2 import service_account
from itertools import groupby

SPREADSHEET_ID = "1LQLM0RjuHQ85YNRI85TH5bXD5N9QxTrF1kUmzrBwcVc"
RANGE_NAME = "Arkusz1"


class GoogleSheetsUpdater:
    def __init__(self, spreadsheet_id, range_name):
        self.spreadsheet_id = spreadsheet_id
        self.range_name = range_name
        self.creds = self.get_credentials()
        self.client = gspread.authorize(self.creds)
        self.sheet = self.client.open_by_key(spreadsheet_id).worksheet(range_name)

    def get_credentials(self):
        # Path to your service account JSON key file
        key_file_path = 'credentials.json'

        # Create credentials from the service account JSON key
        credentials = service_account.Credentials.from_service_account_file(
            key_file_path,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        return credentials

    def get_existing_order_ids(self):
        """Fetch existing order_id from GoogleSpreadsheet."""
        existing_orders = self.sheet.col_values(1)[1:]  # Assuming the order IDs are in column A, starting from row 2
        return [str(order).strip() for order in existing_orders if order.isdigit()]

    def sort_spreadsheet(self):
        """Sort orders in GoogleSpreadsheet according to delivery_date and remove rows with empty date strings."""
        data = self.sheet.get_all_values()

        # Save the header row
        header_row = data[0]

        # Remove the header row from data
        data = data[1:]

        # Remove empty rows
        data = [row for row in data if any(row)]

        # Sort the data by delivery_date
        sorted_data = sorted(data, key=lambda x: x[1])  # Assuming delivery_date is at index 1

        # Group the sorted data by delivery_date
        grouped_data = []
        for date, group in groupby(sorted_data, key=lambda x: x[1]):
            group_list = list(group)
            group_list.sort(key=lambda x: x[4])  # Assuming column E is the shipping address
            grouped_data.extend(group_list)

        # Insert the header row back into the grouped data
        grouped_data.insert(0, header_row)

        # Update the Google Spreadsheet with the sorted data, starting from the second row
        self.update_spreadsheet(grouped_data[1:])  # Exclude the header row

    def update_spreadsheet(self, sorted_data):
        """Update Google Spreadsheet with sorted data."""
        num_rows = len(sorted_data)
        num_cols = len(sorted_data[0])

        # Get current dimensions of the sheet
        current_rows, current_cols = self.sheet.row_count, self.sheet.col_count

        # Resize the sheet only if the new size is larger
        if num_rows > current_rows or num_cols > current_cols:
            self.sheet.resize(max(num_rows, current_rows), max(num_cols, current_cols))

        # Update the sheet with sorted data, starting from the second row
        cell_list = self.sheet.range(2, 1, num_rows + 1, num_cols)  # Adjusted to start from the second row
        for row_index, row_data in enumerate(sorted_data):
            for col_index, value in enumerate(row_data):
                cell_list[row_index * num_cols + col_index].value = value  # Adjusted index calculation
        self.sheet.update_cells(cell_list)

    def update_data(self, new_data):
        """Update orders in GoogleSpreadsheet."""
        # Check if the true stands that the second column of new_data is not empty
        if new_data[1] is not None:
            num_rows = len(self.sheet.get_all_values())
            new_range = f'A{num_rows + 1}'
            self.sheet.append_rows([new_data], value_input_option='RAW', table_range=new_range)


updater = GoogleSheetsUpdater(SPREADSHEET_ID, RANGE_NAME)

# Check if order with specified order_id had been already added to spreadsheet.                                    DONE
# Check the latest order_id in spreadsheet and add every other which has not been added.                           DONE
# Add fetching data from DB about _pa_topper, _pa_swieczka-nr-1 and _pa_swieczka-nr-2
# and putting it into spreadsheet                                                                                  DONE
# Add logic that position orders with chronological delivery_date                                                  DONE
# Add test cases for making script unreliable                                                               IN PROGRESS
# Change the way of passing credentials to database in script (hash it, or make it found by os.path?)       IN PROGRESS
