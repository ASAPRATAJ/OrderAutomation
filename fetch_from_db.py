"""Script for fetching data from MySQL database."""
import mysql.connector


class MySQLDataFetcher:
    def __init__(self, username, password, host, database):
        self.conn = mysql.connector.connect(
            user=username,
            password=password,
            host=host,
            database=database
        )
        self.cur = self.conn.cursor()

    def get_latest_order_id(self):
        order_id_query = "SELECT post_id FROM wp_postmeta ORDER BY post_id DESC LIMIT 1"
        self.cur.execute(order_id_query)
        order_id_result = self.cur.fetchone()
        return order_id_result[0] if order_id_result else None

    def get_product_names_and_quantities(self, order_id):
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
        delivery_date_query = "SELECT wim_delivery_date.meta_value AS delivery_date " \
                              "FROM blueluna_polishlody_test.wp_woocommerce_order_itemmeta wim_delivery_date " \
                              "WHERE wim_delivery_date.order_item_id = ( " \
                              "SELECT woi.order_item_id " \
                              "FROM blueluna_polishlody_test.wp_woocommerce_order_items woi " \
                              "JOIN wp_postmeta pm ON woi.order_id = pm.post_id " \
                              "WHERE woi.order_item_type = 'shipping' AND pm.post_id = %s LIMIT 1) " \
                              "AND wim_delivery_date.meta_key = '_delivery_date'"
        self.cur.execute(delivery_date_query, (order_id,))
        result = self.cur.fetchall()
        date = result[0][0]
        return date

    def get_shipping_address(self, order_id):
        shipping_address_query = "SELECT woi.order_item_id, " \
                                 "CASE WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN woi.order_item_name " \
                                 "ELSE NULL " \
                                 "END AS order_item_name, " \
                                 "CASE WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN ( " \
                                 "SELECT meta_value " \
                                 "FROM blueluna_polishlody_test.wp_postmeta " \
                                 "WHERE post_id = woi.order_id AND meta_key = '_billing_address_1') " \
                                 "ELSE woi.order_item_name " \
                                 "END AS billing_address_1, " \
                                 "CASE " \
                                 "WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN ( " \
                                 "SELECT meta_value " \
                                 "FROM blueluna_polishlody_test.wp_postmeta " \
                                 "WHERE post_id = woi.order_id AND meta_key = '_billing_city') " \
                                 "ELSE NULL " \
                                 "END AS billing_city, " \
                                 "CASE WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN( " \
                                 "SELECT meta_value " \
                                 "FROM blueluna_polishlody_test.wp_postmeta " \
                                 "WHERE post_id = woi.order_id AND meta_key = '_billing_post_code') " \
                                 "ELSE NULL " \
                                 "END AS billing_post_code, " \
                                 "CASE " \
                                 "WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia' THEN( " \
                                 "SELECT meta_value " \
                                 "FROM blueluna_polishlody_test.wp_postmeta " \
                                 "WHERE post_id = woi.order_id AND meta_key = '_billing_phone') " \
                                 "ELSE NULL " \
                                 "END AS billing_phone " \
                                 "FROM blueluna_polishlody_test.wp_woocommerce_order_items woi " \
                                 "WHERE woi.order_item_type = 'shipping' AND woi.order_id = %s"

        self.cur.execute(shipping_address_query, (order_id,))
        result = self.cur.fetchall()
        if result and result[0][3] is not None:
            shipping_data = ", ".join(f'{street_name}, {city_name}, \ntelefon kontaktowy: {phone_number}' for
                                      order_id, shipping_method, street_name, city_name, postal_code, phone_number in
                                      result)
            return shipping_data
        else:
            shipping_data = result[0][2]
            return shipping_data

    def get_comments_to_order(self, order_id):
        comments_to_order_query = "SELECT post_excerpt " \
                                  "FROM blueluna_polishlody_test.wp_posts " \
                                  "WHERE ID = %s "
        self.cur.execute(comments_to_order_query, (order_id,))

        return self.cur.fetchone()[0]

    def get_first_and_last_name(self, order_id):
        first_and_last_name_query = "SELECT " \
                                    "MAX(CASE WHEN meta_key = '_billing_first_name' THEN meta_value END) AS billing_first_name, " \
                                    "MAX(CASE WHEN meta_key = '_billing_last_name' THEN meta_value END) AS billing_last_name " \
                                    "FROM blueluna_polishlody_test.wp_postmeta " \
                                    "WHERE post_id = %s"
        self.cur.execute(first_and_last_name_query, (order_id,))
        result = self.cur.fetchall()
        name_data = " ".join(f'{first_name} {last_name}' for first_name, last_name in result)
        return name_data

    def get_product_price(self, order_id):
        product_price_query = "SELECT post_id AS order_id, " \
                              "MAX(CASE WHEN meta_key = '_order_total' THEN meta_value END) - MAX(CASE WHEN meta_key = '_order_shipping' THEN meta_value END) AS cake_price " \
                              "FROM blueluna_polishlody_test.wp_postmeta " \
                              "WHERE post_id = %s AND meta_key IN ('_order_total', '_order_shipping') " \
                              "GROUP BY post_id"
        self.cur.execute(product_price_query, (order_id,))
        result = self.cur.fetchall()

        cake_price = " ".join(f'{product_name} zł' for order_id, product_name in result)
        return cake_price

    def get_shipping_price(self, order_id):
        shipping_price_query = "SELECT post_id AS order_id, meta_value AS order_shipping " \
                               "FROM blueluna_polishlody_test.wp_postmeta " \
                               "WHERE post_id = %s AND meta_key = '_order_shipping'"
        self.cur.execute(shipping_price_query, (order_id,))
        return self.cur.fetchall()[0][1]

    def get_payment_method(self, order_id):
        payment_method_query = "SELECT post_id AS order_id, " \
                               "MAX(CASE WHEN meta_key = '_payment_method_title' THEN meta_value END) AS payment_method_title " \
                               "FROM blueluna_polishlody_test.wp_postmeta " \
                               "WHERE post_id = %s AND meta_key = '_payment_method_title'"
        self.cur.execute(payment_method_query, (order_id,))
        return self.cur.fetchall()[0][1]

    def get_order_attributes(self, order_id):
        pass
    # def fetch_data(self):
    #     order_id = self.get_latest_order_id()
    #     if order_id is not None:
    #         product_name_result = self.get_product_names_and_quantities(order_id)
    #         delivery_date_result = self.get_delivery_date(order_id)
    #         return order_id, product_name_result, delivery_date_result
    #     else:
    #         return None

    def close_connection(self):
        self.cur.close()
        self.conn.close()


                                        ### Need to change it to os.path ###
data_fetcher = MySQLDataFetcher(
    username='blueluna_polishlody_raport',
    password='+7ubV3m*cnW_',
    host='mn09.webd.pl',
    database='blueluna_polishlody_test'
)


order_id = data_fetcher.get_latest_order_id()
product_name_and_quantities = data_fetcher.get_product_names_and_quantities(order_id)
delivery_date = data_fetcher.get_delivery_date(order_id)
shipping_address = data_fetcher.get_shipping_address(order_id)
comments_to_order = data_fetcher.get_comments_to_order(order_id)
first_and_last_name = data_fetcher.get_first_and_last_name(order_id)
product_price = data_fetcher.get_product_price(order_id)
shipping_price = data_fetcher.get_shipping_price(order_id)
payment_method = data_fetcher.get_payment_method(order_id)


print(order_id)
print(product_name_and_quantities)
print(delivery_date)
print(shipping_address)
print(comments_to_order)
print(first_and_last_name)
print(product_price)
print(shipping_price)
print(payment_method)
data_fetcher.close_connection()
