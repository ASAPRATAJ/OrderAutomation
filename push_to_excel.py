"""Script for updating data in google spreadsheets."""
import os.path
import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

spreadsheet_id = "1vJoqb1CHhk7RybF5Ikw6aKd8vlK6fEbDm538vjzPcCY"
range_name = "Arkusz1!A:J"


class GoogleSheetsUpdater:
    def __init__(self, spreadsheet_id, range_name):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.spreadsheet_id = spreadsheet_id
        self.range_name = range_name
        self.creds = self.get_credentials()
        self.service = build("sheets", "v4", credentials=self.creds)

    def get_credentials(self):
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
        values_range = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"{self.range_name.split('!')[0]}!A5:A"
        ).execute()

        existing_orders = values_range.get('values') if 'values' in values_range else []

        return [str(order[0]).strip() for order in existing_orders if
                order]  # Zamień na str i usuń ewentualne białe znaki

    def update_data(self, new_data):
        # Pobierz aktualny zakres arkusza
        values_range = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=self.range_name
        ).execute()

        # Sprawdź ilość aktualnych wierszy
        num_rows = len(values_range.get('values')) if 'values' in values_range else 0

        # Tworzy nowy zakres, zaczynając od następnego wiersza
        new_range = f"{self.range_name.split('!')[0]}!A{num_rows + 2}:J{num_rows + 2 + len(new_data) - 1}"
        # Aktualizuj nowy zakres z nowymi danymi
        body = {"values": new_data}
        result = self.service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=new_range,
            valueInputOption="RAW",
            body=body
        ).execute()

        print(f"{result.get('updatedCells')} cells updated.")

    def check_if_order_had_been_already_added(self, order_id):
        values_range = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=self.range_name
        ).execute()

        existing_orders = values_range.get('values') if 'values' in values_range else []

        for row in existing_orders:
            if row and str(row[0]).strip() == str(order_id).strip():
                return True  # Order already exists in spreadsheet

        return False  # Order is not exists in spreadsheet

    def skip_or_add_data(self, new_data):
        order_id = new_data[0]

        if not self.check_if_order_had_been_already_added(order_id):
            # Zamówienie nie istnieje, więc dodaj je
            self.update_data([new_data])
            print(f"Order with order_id {order_id} added to the spreadsheet.")
        else:
            print(f"Order with order_id {order_id} already exists in the spreadsheet. Skipped.")


# Check if order with specified order_id had been already added to spreadsheet.                                    DONE
# Check the latest order_id in spreadsheet and add every other which has not been added.                    IN PROGRESS
# Add test cases for making script unreliable                                                               IN PROGRESS
# Change the way of passing credentials to database in script (hash it, or make it found by os.path?)       IN PROGRESS


updater = GoogleSheetsUpdater(spreadsheet_id, range_name)
