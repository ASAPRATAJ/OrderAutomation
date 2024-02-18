"""Script for fetching data from MySQL database."""
import mysql.connector


class MySQLDataFetcher:
    def __init__(self, username, password, host, database):
        """Init arguments passed to the class - connecting to db data."""

        self.conn = mysql.connector.connect(
            user=username,
            password=password,
            host=host,
            database=database
        )
        self.cur = self.conn.cursor()

    def get_latest_order_id(self):
        """SQL query for fetching latest order_id from db."""

        order_id_query = "SELECT post_id FROM wp_postmeta ORDER BY post_id DESC LIMIT 1"
        self.cur.execute(order_id_query)
        order_id_result = self.cur.fetchone()
        return order_id_result[0] if order_id_result else None

    def get_product_names_and_quantities(self, order_id):
        """SQL query for fetching product name and its quantity from db."""

        product_name_query = "SELECT DISTINCT woi.order_item_name, wim.meta_value AS quantity " \
                             "FROM wp_woocommerce_order_items woi " \
                             "JOIN wp_postmeta pm ON woi.order_id = pm.post_id " \
                             "JOIN wp_woocommerce_order_itemmeta wim ON woi.order_item_id = wim.order_item_id " \
                             "WHERE woi.order_item_type = 'line_item' AND pm.post_id = %s AND wim.meta_key = '_qty'"
        self.cur.execute(product_name_query, (order_id,))
        result = self.cur.fetchall()
        order_details = ', \n'.join(f'{product_name} ({quantity} szt.)' for product_name, quantity in result)

        return order_details

    def get_delivery_date(self, order_id):
        """SQL query for fetching delivery date from db."""

        delivery_date_query = "SELECT wim_delivery_date.meta_value AS delivery_date " \
                              "FROM blueluna_polishlody.wp_woocommerce_order_itemmeta wim_delivery_date " \
                              "WHERE wim_delivery_date.order_item_id = ( " \
                              "SELECT woi.order_item_id " \
                              "FROM blueluna_polishlody.wp_woocommerce_order_items woi " \
                              "JOIN wp_postmeta pm ON woi.order_id = pm.post_id " \
                              "WHERE woi.order_item_type = 'shipping' AND pm.post_id = %s LIMIT 1) " \
                              "AND wim_delivery_date.meta_key = '_delivery_date'"
        self.cur.execute(delivery_date_query, (order_id,))
        result = self.cur.fetchall()
        if result:
            date = result[0][0]
            return date
        else:
            return None

    def get_shipping_address(self, order_id):
        """SQL query for fetching shipping address if exists in db"""

        shipping_address_query = "SELECT woi.order_item_id, " \
                                 "CASE WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN woi.order_item_name " \
                                 "ELSE NULL " \
                                 "END AS order_item_name, " \
                                 "CASE WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN ( " \
                                 "SELECT meta_value " \
                                 "FROM blueluna_polishlody.wp_postmeta " \
                                 "WHERE post_id = woi.order_id AND meta_key = '_billing_address_1') " \
                                 "ELSE woi.order_item_name " \
                                 "END AS billing_address_1, " \
                                 "CASE WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN ( " \
                                 "SELECT meta_value " \
                                 "FROM blueluna_polishlody.wp_postmeta " \
                                 "WHERE post_id = woi.order_id AND meta_key = '_billing_address_2') " \
                                 "ELSE NULL " \
                                 "END AS billing_address_2, " \
                                 "CASE WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN ( " \
                                 "SELECT meta_value " \
                                 "FROM blueluna_polishlody.wp_postmeta " \
                                 "WHERE post_id = woi.order_id AND meta_key = '_billing_city') " \
                                 "ELSE NULL " \
                                 "END AS billing_city, " \
                                 "CASE WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN( " \
                                 "SELECT meta_value " \
                                 "FROM blueluna_polishlody.wp_postmeta " \
                                 "WHERE post_id = woi.order_id AND meta_key = '_billing_post_code') " \
                                 "ELSE NULL " \
                                 "END AS billing_post_code, " \
                                 "CASE " \
                                 "WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN( " \
                                 "SELECT meta_value " \
                                 "FROM blueluna_polishlody.wp_postmeta " \
                                 "WHERE post_id = woi.order_id AND meta_key = '_billing_phone') " \
                                 "ELSE NULL " \
                                 "END AS billing_phone " \
                                 "FROM blueluna_polishlody.wp_woocommerce_order_items woi " \
                                 "WHERE woi.order_item_type = 'shipping' AND woi.order_id = %s"

        self.cur.execute(shipping_address_query, (order_id,))
        result = self.cur.fetchall()

        if result:
            if result and result[0][3] or result[0][4] is not None:
                shipping_data = ", ".join(f'{street_name} {street_name_number}, {city_name}, \ntelefon kontaktowy: {phone_number}' for
                                          order_id, shipping_method, street_name, street_name_number, city_name, postal_code, phone_number in
                                          result)
                return shipping_data
            elif result:
                shipping_data = result[0][2]
                return shipping_data
            else:
                return None

    def get_comments_to_order(self, order_id):
        """SQL query for fetching comments included in order (for example specified delivery time)."""

        comments_to_order_query = "SELECT post_excerpt " \
                                  "FROM blueluna_polishlody.wp_posts " \
                                  "WHERE ID = %s "
        self.cur.execute(comments_to_order_query, (order_id,))
        result = self.cur.fetchone()
        if result:
            return result[0]
        else:
            return None

    def get_first_and_last_name(self, order_id):
        """SQL query for fetching full name of client."""

        first_and_last_name_query = "SELECT " \
                                    "MAX(CASE WHEN meta_key = '_billing_first_name' THEN meta_value END) AS billing_first_name, " \
                                    "MAX(CASE WHEN meta_key = '_billing_last_name' THEN meta_value END) AS billing_last_name " \
                                    "FROM blueluna_polishlody.wp_postmeta " \
                                    "WHERE post_id = %s"
        self.cur.execute(first_and_last_name_query, (order_id,))
        result = self.cur.fetchall()
        name_data = " ".join(f'{first_name} {last_name}' for first_name, last_name in result)
        return name_data

    def get_product_price(self, order_id):
        """SQL query for fetching only product price"""

        product_price_query = "SELECT post_id AS order_id, " \
                              "MAX(CASE WHEN meta_key = '_order_total' THEN meta_value END) - MAX(CASE WHEN meta_key = '_order_shipping' THEN meta_value END) AS cake_price " \
                              "FROM blueluna_polishlody.wp_postmeta " \
                              "WHERE post_id = %s AND meta_key IN ('_order_total', '_order_shipping') " \
                              "GROUP BY post_id"
        self.cur.execute(product_price_query, (order_id,))
        result = self.cur.fetchall()

        cake_price = " ".join(f'{product_name} zł' for order_id, product_name in result)
        return cake_price

    def get_shipping_price(self, order_id):
        """SQL query for fetching shipping price, only if order is shipped"""

        shipping_price_query = "SELECT post_id AS order_id, meta_value AS order_shipping " \
                               "FROM blueluna_polishlody.wp_postmeta " \
                               "WHERE post_id = %s AND meta_key = '_order_shipping'"
        self.cur.execute(shipping_price_query, (order_id,))
        result = self.cur.fetchall()
        if result:
            return result[0][1]
        else:
            return None

    def get_payment_method(self, order_id):
        """SQL query for fetching payment method."""

        payment_method_query = "SELECT post_id AS order_id, " \
                               "MAX(CASE WHEN meta_key = '_payment_method_title' THEN meta_value END) AS payment_method_title " \
                               "FROM blueluna_polishlody.wp_postmeta " \
                               "WHERE post_id = %s AND meta_key = '_payment_method_title'"
        self.cur.execute(payment_method_query, (order_id,))
        return self.cur.fetchall()[0][1]

    def get_order_attributes(self, order_id):
        """SQL query for fetching order attributes (f.e. topper, candles)."""

        order_attributes_query = "SELECT " \
                                 "woi.order_id, " \
                                 "woi.order_item_id, " \
                                 "MAX(CASE WHEN wim.meta_key = 'pa_topper' THEN wim.meta_value END) AS pa_topper, " \
                                 "MAX(CASE WHEN wim.meta_key = 'pa_swieczka-nr-1' THEN wim.meta_value END) " \
                                 "AS pa_swieczka_nr_1, " \
                                 "MAX(CASE WHEN wim.meta_key = 'pa_swieczka-nr-2' THEN wim.meta_value END) " \
                                 "AS pa_swieczka_nr_2, " \
                                 "MAX(CASE WHEN wim.meta_key = 'warstwa-1' THEN wim.meta_value END) AS warstwa_1, " \
                                 "MAX(CASE WHEN wim.meta_key = 'warstwa-2' THEN wim.meta_value END) AS warstwa_2, " \
                                 "MAX(CASE WHEN wim.meta_key = 'warstwa-3' THEN wim.meta_value END) AS warstwa_3, " \
                                 "MAX(CASE WHEN wim.meta_key = 'warstwa-4' THEN wim.meta_value END) AS warstwa_4, " \
                                 "MAX(CASE WHEN wim.meta_key = 'dekoracja' THEN wim.meta_value END) AS dekoracja " \
                                 "FROM blueluna_polishlody.wp_woocommerce_order_items woi " \
                                 "JOIN blueluna_polishlody.wp_woocommerce_order_itemmeta wim ON woi.order_item_id = " \
                                 "wim.order_item_id " \
                                 "WHERE woi.order_id = %s " \
                                 "AND wim.meta_key IN ('pa_topper', 'pa_swieczka-nr-1', 'pa_swieczka-nr-2', " \
                                 "'warstwa-1', 'warstwa-2', 'warstwa-3', 'warstwa-4', 'dekoracja')"
        self.cur.execute(order_attributes_query, (order_id,))
        result = self.cur.fetchall()
        if result[0][5] is not None:
            order_diy = " ".join(
                f'{warstwa_1}, \n{warstwa_2}, \n{warstwa_3}, \n{warstwa_4}, \ndekoracja:{dekoracja}' for
                order_id, order_item_id, topper, swieczka_nr_1, swieczka_nr_2, warstwa_1, warstwa_2, warstwa_3,
                warstwa_4, dekoracja in result)
            return order_diy
        elif result:
            order_attributes = " ".join(
                f'{topper}, \n{swieczka_nr_1}, \n{swieczka_nr_2}' for
                order_id, order_item_id, topper, swieczka_nr_1, swieczka_nr_2, warstwa_1, warstwa_2, warstwa_3,
                warstwa_4, dekoracja in result)
            return order_attributes

    def get_missing_order_ids(self, existing_order_ids):
        """Method for checking if every order in db is also in spreadsheet."""

        # If there is not any existing orders in spreadsheet, then return all fetched orders from db.
        if not existing_order_ids:
            latest_order_id = self.get_latest_order_id()
            if latest_order_id is not None:
                all_order_ids = range(5500,
                                      latest_order_id + 1)  # There you can add order_id range which should be first in
                #                                                                                           spreadsheet.
                return all_order_ids
            else:
                return []

        max_existing_order_id = max(map(int, existing_order_ids), default=0)

        latest_order_id = self.get_latest_order_id()
        if latest_order_id is not None:
            missing_order_ids = [order_id for order_id in range(max_existing_order_id + 1, latest_order_id + 1)]
            return missing_order_ids
        else:
            return []

    def close_connection(self):
        self.cur.close()
        self.conn.close()

        ### Need to change it to os.path ###


data_fetcher = MySQLDataFetcher(
    username='blueluna_polishlody_raport_prod',
    password='pV}]^?B90q83',
    host='mn09.webd.pl',
    database='blueluna_polishlody'
)
