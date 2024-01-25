"""Script for updating data in google spreadsheets."""
import os.path
import fetch_from_db
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


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


if __name__ == '__main__':
    spreadsheet_id = "1vJoqb1CHhk7RybF5Ikw6aKd8vlK6fEbDm538vjzPcCY"
    range_name = "Arkusz1!A:J"

    updater = GoogleSheetsUpdater(spreadsheet_id, range_name)
    data_fetcher = fetch_from_db.MySQLDataFetcher(
        username='blueluna_polishlody_raport',
        password='+7ubV3m*cnW_',
        host='mn09.webd.pl',
        database='blueluna_polishlody_test'
    )

    order_id = data_fetcher.get_latest_order_id()
    product_names_and_quantities = data_fetcher.get_product_names_and_quantities(order_id)
    delivery_date = data_fetcher.get_delivery_date(order_id)
    shipping_address = data_fetcher.get_shipping_address(order_id)
    comments_to_order = data_fetcher.get_comments_to_order(order_id)
    first_and_last_name = data_fetcher.get_first_and_last_name(order_id)
    product_price = data_fetcher.get_product_price(order_id)
    shipping_price = data_fetcher.get_shipping_price(order_id)
    payment_method = data_fetcher.get_payment_method(order_id)
    order_attributes = data_fetcher.get_order_attributes(order_id)

    new_data = [
        order_id,
        delivery_date,
        product_names_and_quantities,
        order_attributes,
        shipping_address,
        product_price,
        shipping_price,
        payment_method,
        first_and_last_name,
        comments_to_order,
    ]

    updater.update_data([new_data])
    data_fetcher.close_connection()
