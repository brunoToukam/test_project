import csv
import os
import shutil
import tempfile
import unittest

from src.data_processor_multi import DataProcessor


class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        print("Setting up the test environment.")
        # Initialize with the test file
        script_dir = os.path.dirname(__file__)
        self.output_dir = os.path.join(script_dir, 'test_results')
        self.temp_dir = os.path.join(script_dir, 'temp_results')
        test_file_path = os.path.join(script_dir, 'test_data', 'test_file.psv')

        self.processor = DataProcessor(test_file_path, self.output_dir, num_processes=2)

    def tearDown(self):
        # Clean up after each test
        for filename in os.listdir(self.processor.temp_dir):
            os.remove(os.path.join(self.processor.temp_dir, filename))
        os.rmdir(self.processor.temp_dir)
        # shutil.rmtree(self.test_temp_dir)

    def test_top_50_stores_by_total_price(self):
        self.processor.top_50_stores_by_total_price()
        output_file = os.path.join(self.output_dir, 'top_50_stores.csv')
        # Verify the total price per store
        expected_totals = {'76094': 38.50, '74339': 20.25, '83277': 18.44, '38185': 9.25}
        # Assuming the output is in 'top_50_stores_by_total_price.csv'
        with open(output_file, 'r') as file:
            reader = csv.reader(file)
            next(reader)
            for row in reader:
                print(row)
                store_code, total_price = row[0], float(row[1])
                self.assertEqual(expected_totals[store_code], total_price)

    def test_top_100_products_by_store(self):
        self.processor.top_100_products_by_store()
        # Verify the top products per store
        expected_products = {
            '76094': {'F2762F2854': 3, 'F2762F2853': 2},
            '38185': {'F2762F2854': 1},
            '74339': {'F2762F2851': 1, 'F2762F2854': 1},
            '83277': {'88367280C6': 3}
        }
        for store_code, expected_counts in expected_products.items():
            output_file = os.path.join(self.output_dir, 'top-100-by-store', f'top-100-products-store-{store_code}.csv')
            with open(output_file, 'r') as file:
                reader = csv.reader(file)
                next(reader)  # Skip header
                actual_counts = {rows[0]: int(rows[1]) for rows in reader}
                for product_id, count in expected_counts.items():
                    self.assertEqual(actual_counts.get(product_id, 0), count)


if __name__ == '__main__':
    unittest.main()
