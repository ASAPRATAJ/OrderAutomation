"""Script for updating data in google spreadsheets."""
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SPREADSHEET_ID = "1vJoqb1CHhk7RybF5Ikw6aKd8vlK6fEbDm538vjzPcCY"
RANGE_NAME = "Arkusz1!A:J"


class GoogleSheetsUpdater:
    def __init__(self, spreadsheet_id, range_name):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.spreadsheet_id = spreadsheet_id
        self.range_name = range_name
        self.creds = self.get_credentials()
        self.service = build("sheets", "v4", credentials=self.creds)

    def get_credentials(self):
        """Get credentials and create token if does not exist."""
        creds = None
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", self.scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", self.scopes)
                creds = flow.run_local_server(port=0)

            with open("token.json", "w") as token:
                token.write(creds.to_json())

        return creds

    def get_existing_order_ids(self):
        """Fetch existing order_id from GoogleSpreadsheet."""
        values_range = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.range_name.split('!')[0]}!A5:A"
        ).execute()

        existing_orders = values_range.get('values') if 'values' in values_range else []

        return [str(order[0]).strip() for order in existing_orders if
                order]

    def sort_spreadsheet(self):
        """Sort orders in GoogleSpreadsheet according to delivery_date."""
        values_range = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=self.range_name
        ).execute()

        values = values_range.get('values') if 'values' in values_range else []

        # Sort the values based on the delivery date (assuming it's in the second column and data starts in fourth row)
        sorted_values = sorted(values[4:], key=lambda x: x[1] if x and len(x) > 1 else '', reverse=False)

        # Update the sorted values in the spreadsheet
        new_range = f"{self.range_name.split('!')[0]}!A5:J{len(sorted_values) + 4}"
        body = {"values": sorted_values}
        self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=new_range,
            valueInputOption="RAW",
            body=body,
        ).execute()

    def update_data(self, new_data):
        """Update orders in GoogleSpreadsheet."""
        values_range = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=self.range_name
        ).execute()

        # Check the number of rows
        num_rows = len(values_range.get('values')) if 'values' in values_range else 0

        # Create new range, starting from new row
        new_range = f"{self.range_name.split('!')[0]}!A{num_rows + 2}:J{num_rows + 1 + len(new_data) - 1}"
        # Update new range with new order data
        body = {"values": new_data}
        result = self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=new_range,
            valueInputOption="RAW",
            body=body
        ).execute()

        # Sort the spreadsheet based on the delivery date column
        self.sort_spreadsheet()

        print(f"{result.get('updatedCells')} cells updated.")

    def check_if_order_had_been_already_added(self, order_id):
        """Check if fetched order is not already in spreadsheet."""
        values_range = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=self.range_name
        ).execute()

        existing_orders = values_range.get('values') if 'values' in values_range else []

        for row in existing_orders:
            if row and str(row[0]).strip() == str(order_id).strip():
                return True  # Order already exists in spreadsheet

        return False  # Order does not exist in spreadsheet

    def skip_or_add_data(self, new_data):
        """Method connected with update data and check data"""
        order_id = new_data[0]

        if not self.check_if_order_had_been_already_added(order_id):
            # Order does not exist, so it will be added
            self.update_data([new_data])
            print(f"Order with order_id {order_id} added to the spreadsheet.")
        else:
            print(f"Order with order_id {order_id} already exists in the spreadsheet. Skipped.")


updater = GoogleSheetsUpdater(SPREADSHEET_ID, RANGE_NAME)

# Check if order with specified order_id had been already added to spreadsheet.                                    DONE
# Check the latest order_id in spreadsheet and add every other which has not been added.                           DONE
# Add fetching data from DB about _pa_topper, _pa_swieczka-nr-1 and _pa_swieczka-nr-2
# and putting it into spreadsheet                                                                                  DONE
# Add logic that position orders with chronological delivery_date                                                  DONE
# Add test cases for making script unreliable                                                               IN PROGRESS
# Change the way of passing credentials to database in script (hash it, or make it found by os.path?)       IN PROGRESS


