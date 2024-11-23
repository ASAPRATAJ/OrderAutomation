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

        order_id_query = """
            SELECT 
                post_id 
            FROM 
                wp_postmeta 
            ORDER BY 
                post_id DESC 
            LIMIT 1
        """
        self.cur.execute(order_id_query)
        order_id_result = self.cur.fetchone()
        return order_id_result[0] if order_id_result else None

    def get_product_names_and_quantities(self, order_id):
        """SQL query for fetching product name and its quantity from db."""

        # Query to fetch product names and quantities for line items
        product_name_query = """
                SELECT 
                    woi.order_item_name, 
                    wim.meta_value AS quantity 
                FROM 
                    wp_woocommerce_order_items woi 
                JOIN 
                    wp_woocommerce_order_itemmeta wim ON woi.order_item_id = wim.order_item_id 
                WHERE 
                    woi.order_id = %s 
                    AND woi.order_item_type = 'line_item' 
                    AND wim.meta_key = '_qty'
            """

        self.cur.execute(product_name_query, (order_id,))
        result = self.cur.fetchall()

        # Dictionary to store product names and their quantities
        product_quantities = {}

        for product_name, quantity in result:
            if product_name not in product_quantities:
                product_quantities[product_name] = int(quantity)
            else:
                product_quantities[product_name] += int(quantity)

        # Prepare order details string with aggregated product names and quantities
        order_details = ', \n'.join(
            f'{product_name} ({quantity} szt.)' for product_name, quantity in product_quantities.items())

        return order_details

    def get_delivery_date(self, order_id):
        """SQL query for fetching delivery date from db."""

        delivery_date_query = """
            SELECT 
                wim_delivery_date.meta_value AS delivery_date 
            FROM 
                blueluna_polishlody.wp_woocommerce_order_itemmeta wim_delivery_date 
            WHERE 
                wim_delivery_date.order_item_id = (
                    SELECT 
                        woi.order_item_id 
                    FROM 
                        blueluna_polishlody.wp_woocommerce_order_items woi 
                        JOIN wp_postmeta pm ON woi.order_id = pm.post_id 
                    WHERE 
                        woi.order_item_type = 'shipping' AND pm.post_id = %s 
                    LIMIT 1
                ) 
                AND wim_delivery_date.meta_key = '_delivery_date'
        """

        self.cur.execute(delivery_date_query, (order_id,))
        result = self.cur.fetchall()
        if result:
            date = result[0][0]
            return date
        else:
            return None

    def get_shipping_address(self, order_id):
        """SQL query for fetching shipping address if exists in db"""

        shipping_address_query = """
            SELECT 
                woi.order_item_id,
                CASE 
                    WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia - dostarczamy torty autem z mroźnią' THEN woi.order_item_name 
                    ELSE NULL 
                END AS order_item_name,
                CASE 
                    WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia - dostarczamy torty autem z mroźnią' THEN (
                        SELECT meta_value 
                        FROM blueluna_polishlody.wp_postmeta 
                        WHERE post_id = woi.order_id AND meta_key = '_shipping_address_1'
                    )
                    ELSE woi.order_item_name 
                END AS billing_address_1,
                CASE 
                    WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia - dostarczamy torty autem z mroźnią' THEN (
                        SELECT meta_value 
                        FROM blueluna_polishlody.wp_postmeta 
                        WHERE post_id = woi.order_id AND meta_key = '_shipping_address_2'
                    )
                    ELSE NULL 
                END AS billing_address_2,
                CASE 
                    WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia - dostarczamy torty autem z mroźnią' THEN (
                        SELECT meta_value 
                        FROM blueluna_polishlody.wp_postmeta 
                        WHERE post_id = woi.order_id AND meta_key = '_shipping_city'
                    )
                    ELSE NULL 
                END AS billing_city,
                CASE 
                    WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia - dostarczamy torty autem z mroźnią' THEN (
                        SELECT meta_value 
                        FROM blueluna_polishlody.wp_postmeta 
                        WHERE post_id = woi.order_id AND meta_key = '_shipping_company'
                    )
                    ELSE NULL 
                END AS shipping_company,
                CASE 
                    WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia - dostarczamy torty autem z mroźnią' THEN (
                        SELECT meta_value 
                        FROM blueluna_polishlody.wp_postmeta 
                        WHERE post_id = woi.order_id AND meta_key = '_billing_phone'
                    )
                    ELSE NULL 
                END AS billing_phone,
                CASE 
                    WHEN woi.order_item_name = 'Dostawa na terenie Wrocławia - dostarczamy torty autem z mroźnią' THEN (
                        SELECT meta_value 
                        FROM blueluna_polishlody.wp_postmeta 
                        WHERE post_id = woi.order_id AND meta_key = 'Czas dostawy'
                    )
                    ELSE NULL
                END AS delivery_hour
            FROM 
                blueluna_polishlody.wp_woocommerce_order_items woi 
            WHERE 
                woi.order_item_type = 'shipping' AND woi.order_id = %s
        """

        self.cur.execute(shipping_address_query, (order_id,))
        result = self.cur.fetchall()

        if result:
            if result[0][2] == "Odbiór osobisty - Bema (Bezpłatnie)":
                nip_number = self.get_nip_number(order_id)
                if nip_number is not None:
                    return f"Odbiór Bema\nNIP:{nip_number}"
                return "Odbiór Bema"
            elif result[0][2] == "Odbiór osobisty - Olimpia Port (Bezpłatnie)":
                nip_number = self.get_nip_number(order_id)
                if nip_number is not None:
                    return f"Odbiór Olimpia\nNIP:{nip_number}"
                return "Odbiór Olimpia"
            elif result[0][2] == "Odbiór osobisty - Wroclavia (Bezpłatnie)":
                nip_number = self.get_nip_number(order_id)
                if nip_number is not None:
                    return f"Odbiór Wroclavia\nNIP:{nip_number}"
                return "Odbiór Wroclavia"
            elif result[0][2] == "Odbiór osobisty - Hubska (Bezpłatnie)":
                nip_number = self.get_nip_number(order_id)
                if nip_number is not None:
                    return f"Odbiór Hubska\nNIP:{nip_number}"
                return "Odbiór Hubska"
            elif result[0][2] == "Odbiór osobisty - Oławska (Bezpłatnie)":
                nip_number = self.get_nip_number(order_id)
                if nip_number is not None:
                    return f"Odbiór Oławska\nNIP:{nip_number}"
                return "Odbiór Oławska"
            else:
                # Now check if the true stands that the second element is not None
                if result[0][1] is not None:
                    # Now check if the true stands that the sixth element is not None
                    if result[0][5] is not None:
                        nip_number = self.get_nip_number(order_id)
                        if nip_number is not None:
                            shipping_data = ", ".join(
                                f'Adres dostawy:\n{street_name}, {city_name}, \nGodziny dostawy: {delivery_hour} '
                                f'\ntelefon kontaktowy: {phone_number}, \nfirma: {company_name}, \nNIP:{nip_number}'
                                for
                                order_id, shipping_method, street_name, street_name_number, city_name, company_name,
                                phone_number, delivery_hour
                                in
                                result)
                            return shipping_data
                        else:
                            shipping_data = ", ".join(
                                f'Adres dostawy:\n{street_name}, {city_name}, \nGodziny dostawy: {delivery_hour} '
                                f'\ntelefon kontaktowy: {phone_number}, \nfirma: {company_name}'
                                for
                                order_id, shipping_method, street_name, street_name_number, city_name, company_name,
                                phone_number, delivery_hour
                                in
                                result)
                            return shipping_data
                    # Now check if the true stands that the fourth element is None
                    elif result[0][3] is None:
                        shipping_data = ", ".join(
                            f'Adres dostawy:\n{street_name}, {city_name}, \nGodziny dostawy: {delivery_hour} '
                            f'\ntelefon kontaktowy: {phone_number}' for
                            order_id, shipping_method, street_name, street_name_number, city_name, company_name,
                            phone_number, delivery_hour
                            in
                            result)
                        return shipping_data
                    # Now check if the true stands that the fourth element is not None
                    elif result[0][3] is not None:
                        shipping_data = ", ".join(
                            f'Adres dostawy:\n{street_name} {street_name_number}, {city_name}, '
                            f'\nGodziny dostawy: {delivery_hour}'
                            f'\ntelefon kontaktowy: {phone_number}'
                            for
                            order_id, shipping_method, street_name, street_name_number, city_name, company_name,
                            phone_number, delivery_hour
                            in
                            result)
                        return shipping_data
                else:
                    print("Chyba none", result)

    def get_comments_to_order(self, order_id):
        """SQL query for fetching comments included in order (for example specified delivery time)."""

        comments_to_order_query = """
            SELECT 
                post_excerpt 
            FROM 
                blueluna_polishlody.wp_posts 
            WHERE 
                ID = %s
        """
        self.cur.execute(comments_to_order_query, (order_id,))
        result = self.cur.fetchone()
        if result:
            return result[0]
        else:
            return None

    def get_first_and_last_name(self, order_id):
        """SQL query for fetching full name of client."""

        first_and_last_name_query = """
            SELECT 
                MAX(CASE WHEN meta_key = '_billing_first_name' THEN meta_value END) AS billing_first_name, 
                MAX(CASE WHEN meta_key = '_billing_last_name' THEN meta_value END) AS billing_last_name 
            FROM 
                blueluna_polishlody.wp_postmeta 
            WHERE 
                post_id = %s
        """
        self.cur.execute(first_and_last_name_query, (order_id,))
        result = self.cur.fetchall()

        name_data = " ".join(f'{first_name} {last_name}' for first_name, last_name in result)
        # if result[first name] + result[last_name] is equal to PIXEL.lower() == pixelxl then
        #   return 'Pixel XL'
        return name_data

    def get_product_price(self, order_id):
        """SQL query for fetching only product price"""

        product_price_query = """
            SELECT 
                post_id AS order_id, 
                MAX(CASE WHEN meta_key = '_order_total' THEN meta_value END) - MAX(CASE WHEN meta_key = '_order_shipping' THEN meta_value END) AS cake_price 
            FROM 
                blueluna_polishlody.wp_postmeta 
            WHERE 
                post_id = %s AND meta_key IN ('_order_total', '_order_shipping') 
            GROUP BY 
                post_id
        """
        self.cur.execute(product_price_query, (order_id,))
        result = self.cur.fetchall()
        if result:
            order_total = result[0][1]
            termobox_price = self.get_termobox_price(order_id)
            if termobox_price is None:
                termobox_price = 0
            only_product_price = order_total - termobox_price
            cake_price = f'{only_product_price} zł'
            return cake_price
        else:
            print('Nie ma order_total')

    def get_shipping_price(self, order_id):
        """SQL query for fetching shipping price, only if order is shipped"""

        shipping_price_query = """
            SELECT 
                post_id AS order_id, 
                meta_value AS order_shipping 
            FROM 
                blueluna_polishlody.wp_postmeta 
            WHERE 
                post_id = %s AND meta_key = '_order_shipping'
        """
        self.cur.execute(shipping_price_query, (order_id,))
        result = self.cur.fetchall()
        if result:
            termobox_price = self.get_termobox_price(order_id)
            if termobox_price > 0:
                shipping_price = f'Dostawa: {result[0][1]}\nStyropian: {termobox_price}'
                return shipping_price
            else:
                shipping_price = f'Dostawa: {result[0][1]}'
                return shipping_price
        else:
            return None

    def get_payment_method(self, order_id):
        """SQL query for fetching payment method."""

        payment_method_query = """
            SELECT 
                post_id AS order_id, 
                MAX(CASE WHEN meta_key = '_payment_method_title' THEN meta_value END) AS payment_method_title 
            FROM 
                blueluna_polishlody.wp_postmeta 
            WHERE 
                post_id = %s AND meta_key = '_payment_method_title'
        """
        self.cur.execute(payment_method_query, (order_id,))
        return self.cur.fetchall()[0][1]

    def get_order_attributes(self, order_id):
        """SQL query for fetching order attributes (f.e. topper, candles)."""

        order_attributes_query = """
            SELECT 
                woi.order_item_id, 
                MAX(CASE WHEN wim.meta_key = 'pa_topper' THEN wim.meta_value END) AS pa_topper, 
                MAX(CASE WHEN wim.meta_key = 'pa_swieczka-nr-1' THEN wim.meta_value END) AS pa_swieczka_nr_1, 
                MAX(CASE WHEN wim.meta_key = 'pa_swieczka-nr-2' THEN wim.meta_value END) AS pa_swieczka_nr_2, 
                MAX(CASE WHEN wim.meta_key = 'warstwa-1-najnizsza-warstwa' THEN wim.meta_value END) AS warstwa_1, 
                MAX(CASE WHEN wim.meta_key = 'warstwa-2-srodkowa' THEN wim.meta_value END) AS warstwa_2, 
                MAX(CASE WHEN wim.meta_key = 'warstwa-3-srodkowa' THEN wim.meta_value END) AS warstwa_3, 
                MAX(CASE WHEN wim.meta_key = 'warstwa-4-zewnetrzna-warstwa' THEN wim.meta_value END) AS warstwa_4, 
                MAX(CASE WHEN wim.meta_key = 'dekoracja' THEN wim.meta_value END) AS dekoracja 
            FROM 
                blueluna_polishlody.wp_woocommerce_order_items woi 
                JOIN blueluna_polishlody.wp_woocommerce_order_itemmeta wim ON woi.order_item_id = wim.order_item_id 
            WHERE 
                woi.order_id = %s 
                AND woi.order_item_type = 'line_item'
                AND wim.meta_key IN ('pa_topper', 'pa_swieczka-nr-1', 'pa_swieczka-nr-2', 'warstwa-1-najnizsza-warstwa', 'warstwa-2-srodkowa', 'warstwa-3-srodkowa', 'warstwa-4-zewnetrzna-warstwa', 'dekoracja')
            GROUP BY 
                woi.order_item_id
        """

        self.cur.execute(order_attributes_query, (order_id,))
        result = self.cur.fetchall()

        # List to store order attributes for each line item
        order_attributes_list = []

        for row in result:
            order_item_id = row[0]
            topper = row[1]
            swieczka_nr_1 = row[2]
            swieczka_nr_2 = row[3]
            warstwa_1 = row[4]
            warstwa_2 = row[5]
            warstwa_3 = row[6]
            warstwa_4 = row[7]
            dekoracja = row[8]

            # Prepare order attributes string for the current line item
            order_attributes = ""
            if topper:
                order_attributes += f"Topper: {topper}\n"
            if swieczka_nr_1:
                order_attributes += f"Świeczka nr 1: {swieczka_nr_1}\n"
            if swieczka_nr_2:
                order_attributes += f"Świeczka nr 2: {swieczka_nr_2}\n"

            # Check if there are any layers or decoration for DIY cake
            if warstwa_1 or warstwa_2 or warstwa_3 or warstwa_4 or dekoracja:
                if order_attributes:
                    order_attributes += ", "
                order_attributes += f"Warstwa 1: {warstwa_1}, \nWarstwa 2: {warstwa_2}, " \
                                    f"\nWarstwa 3: {warstwa_3}, \nWarstwa 4: {warstwa_4}, \nDekoracja: {dekoracja}"

            order_attributes_list.append(order_attributes)

        # Combine order attributes for all line items into a single string
        order_details = '\n'.join(order_attributes_list)
        return order_details

    def get_missing_order_ids(self, existing_order_ids):
        """Check if every order in the database is also in the spreadsheet."""
        # Pobierz najnowsze order_id z bazy danych
        latest_order_id = self.get_latest_order_id()
        if latest_order_id is None:
            return []  # Brak danych w bazie

        existing_order_ids = set(map(int, existing_order_ids))

        # Jeśli arkusz jest pusty, zwróć wszystkie zamówienia
        if not existing_order_ids:
            print('W arkuszu nie ma zadnych zamowien')
            return list(range(13190, latest_order_id + 1))

        missing_order_ids = []

        # Sprawdzaj każdy order_id i loguj wynik is_new_order_email_sent_true
        for order_id in range(13190, latest_order_id + 1):
            is_sent = self.is_new_order_email_sent_true(order_id)
            print(f"Order ID: {order_id}, is_new_order_email_sent: {is_sent}")
            if order_id not in existing_order_ids and is_sent:
                missing_order_ids.append(order_id)

        return missing_order_ids

    def is_new_order_email_sent_true(self, order_id):
        """Check if _new_order_email_sent is true for the given order ID."""
        email_sent_query = """
            SELECT COUNT(*) 
            FROM wp_postmeta 
            WHERE post_id = %s AND meta_key = '_new_order_email_sent' AND meta_value = 'true'
        """
        self.cur.execute(email_sent_query, (order_id,))
        result = self.cur.fetchone()
        if result[0] > 0:
            return True
        else:
            return False

    def get_termobox_price(self, order_id):
        termobox_price_query = """
        SELECT oim.meta_value AS _fee_amount
        FROM wp_woocommerce_order_items oi
        JOIN wp_woocommerce_order_itemmeta oim ON oi.order_item_id = oim.order_item_id
        WHERE oi.order_id = %s
        AND oi.order_item_type = 'fee'
        AND oim.meta_key = '_fee_amount'
        """
        ## SPRAWDZIC CZY _FEE_AMOUNT CZY _LINE_TOTAL poniewaz nie wiem czy dostawa zalicza się do _line_total
        self.cur.execute(termobox_price_query, (order_id,))
        result = self.cur.fetchall()
        if len(result) > 0:
            termobox_price = result[0][0]
            print(termobox_price)
            return int(termobox_price)
        else:
            return 0

    def get_nip_number(self, order_id):
        nip_number_query = """
        SELECT meta_value
        FROM blueluna_polishlody.wp_postmeta pm
        WHERE pm.post_id = %s AND meta_key = 'NIP'
        """
        self.cur.execute(nip_number_query, (order_id,))
        result = self.cur.fetchone()

        if result is not None:
            nip_number = result[0]
            return nip_number

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
