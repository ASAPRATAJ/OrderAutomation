"""Script for updating data in google spreadsheets."""
import gspread
from google.oauth2 import service_account
from datetime import datetime

SPREADSHEET_ID = "1vJoqb1CHhk7RybF5Ikw6aKd8vlK6fEbDm538vjzPcCY"
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
        return [str(order).strip() for order in existing_orders if order]

    def sort_spreadsheet(self):
        """Sort orders in GoogleSpreadsheet according to delivery_date and remove rows with empty date strings."""
        values = self.sheet.get_all_values()[1:]  # Assuming data starts from row 2

        # Remove rows with empty date strings in the second column
        values = [row for row in values if
                  row[1].strip()]  # Assuming date strings are in the second column and stripping whitespace

        # Sort the remaining values based on the delivery date
        sorted_values = sorted(values, key=lambda x: datetime.strptime(x[1], '%Y-%m-%d'), reverse=False)

        # Update the entire sheet with the sorted values, starting from cell 'A2'
        self.sheet.update('A2', sorted_values)

    def update_data(self, new_data):
        """Update orders in GoogleSpreadsheet."""
        # Check if the second column of new_data is not empty
        print(new_data[1])
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
