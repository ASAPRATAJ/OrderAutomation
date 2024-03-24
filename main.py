"""Main script for launching fetching data from db and pushing it to google spreadsheets."""
from fetch_from_db import MySQLDataFetcher
from push_to_excel import updater


def main():
    data_fetcher = MySQLDataFetcher(
        username='blueluna_polishlody_raport_prod',
        password='pV}]^?B90q83',
        host='mn09.webd.pl',
        database='blueluna_polishlody'
    )
    print("Start fetching orders from database...")
    existing_order_ids = updater.get_existing_order_ids()
    missing_order_ids = data_fetcher.get_missing_order_ids(existing_order_ids)
    print(f'Missing orders in google spreadsheet: {len(missing_order_ids)}', missing_order_ids)

    for missing_order_id in missing_order_ids:
        product_names_and_quantities = data_fetcher.get_product_names_and_quantities(missing_order_id)
        delivery_date = data_fetcher.get_delivery_date(missing_order_id)
        shipping_address = data_fetcher.get_shipping_address(missing_order_id)
        comments_to_order = data_fetcher.get_comments_to_order(missing_order_id)
        first_and_last_name = data_fetcher.get_first_and_last_name(missing_order_id)
        product_price = data_fetcher.get_product_price(missing_order_id)
        shipping_price = data_fetcher.get_shipping_price(missing_order_id)
        payment_method = data_fetcher.get_payment_method(missing_order_id)
        order_attributes = data_fetcher.get_order_attributes(missing_order_id)

        new_data = [
            missing_order_id,
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
        print(new_data)
        updater.update_data(new_data)
    # data_fetcher.get_product_names_and_quantities(6982)
    # data_fetcher.get_order_attributes(6982)
    data_fetcher.close_connection()
    print("Fetching completed. Closing connection.")
    updater.sort_spreadsheet()
    print("Spreadsheet sorted.")


def hello_pubsub(event, context):
    main()


if __name__ == '__main__':
    hello_pubsub('data', 'context')
