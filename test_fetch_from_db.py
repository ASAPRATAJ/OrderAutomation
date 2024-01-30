"""Tests for fetch_from_db methods."""
import unittest
import mysql.connector
from fetch_from_db import MySQLDataFetcher


class TestFetchFromDB(unittest.TestCase):
    def setUp(self):
        self.db_data = {
            'username': 'blueluna_polishlody_raport',
            'password': '+7ubV3m*cnW_',
            'host': 'mn09.webd.pl',
            'database': 'blueluna_polishlody_test',
        }
        self.conn = mysql.connector.connect(**self.db_data)
        self.cur = self.conn.cursor()

    def tearDown(self):
        self.cur.close()
        self.conn.close()

    def test_fetching_order_id(self):
        """Test fetching order_id from db."""
        latest_order_id_query = "SELECT post_id FROM wp_postmeta ORDER BY post_id DESC LIMIT 1"
        self.cur.execute(latest_order_id_query)
        latest_order_id_result = self.cur.fetchone()[0]

        self.assertIsNotNone(latest_order_id_result)

    def test_fetching_latest_order_id(self):
        """Test fetching latest_order_id from db."""
        latest_order_id_query = "SELECT post_id FROM wp_postmeta ORDER BY post_id DESC LIMIT 1"
        self.cur.execute(latest_order_id_query)
        expected_latest_order_id = self.cur.fetchone()[0]

        data_fetcher = MySQLDataFetcher(**self.db_data)
        actual_latest_order_id = data_fetcher.get_latest_order_id()

        self.assertEqual(actual_latest_order_id, expected_latest_order_id)


if __name__ == '__main__':
    unittest.main()

