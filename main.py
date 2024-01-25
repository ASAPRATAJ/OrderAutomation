"""Main script for launching fetching data from db and pushing it to google spreadsheets."""
import fetch_from_db
import push_to_excel

if __name__ == '__main__':
    spreadsheet_id = "1vJoqb1CHhk7RybF5Ikw6aKd8vlK6fEbDm538vjzPcCY"
    range_name = "Arkusz1!A:J"

    updater = push_to_excel.GoogleSheetsUpdater(spreadsheet_id, range_name)
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

    updater.skip_or_add_data(new_data)
    data_fetcher.close_connection()
