import os
import csv
import multiprocessing
from collections import defaultdict
import pickle


def default_dict_int():
    return defaultdict(int)


def process_file_segment(filename, start, end, chunk_index, temp_dir):
    store_products = defaultdict(default_dict_int)
    store_totals = defaultdict(float)

    with open(filename, 'r') as file:
        if start > 0:  # Move to the start of the next full line if not at the beginning
            file.seek(start - 1)  # Move back one character
            while file.read(1) != '\n':  # Read until the start of the next full line
                pass
        else:
            file.readline()  # Skip header if at the beginning

        lines = file.readlines(end - start) if end else file.readlines()
        for line in lines:
            parts = line.strip().split('|')
            if len(parts) == 6 and parts[2]:
                _, _, product_id, store_code, _, price = parts
                store_products[store_code][product_id] += 1
                store_totals[store_code] += float(price)

    temp_filename = os.path.join(temp_dir, f'temp_results_{chunk_index}.pkl')
    with open(temp_filename, 'wb') as f:
        pickle.dump((store_products, store_totals), f)


class DataProcessor:
    def __init__(self, filename, output_dir, num_processes=None):
        self.filename = filename
        self.output_dir = output_dir
        self.num_processes = num_processes or multiprocessing.cpu_count()
        self.temp_dir = os.path.join(output_dir, 'temp_results')
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)

    def combine_results(self, num_chunks):
        combined_products_by_store = defaultdict(default_dict_int)
        combined_stores = defaultdict(float)
        for i in range(num_chunks):
            temp_filename = os.path.join(self.temp_dir, f'temp_results_{i}.pkl')
            if os.path.exists(temp_filename) and os.path.getsize(temp_filename) > 0:
                with open(temp_filename, 'rb') as f:
                    store_products, store_totals = pickle.load(f)
                    for store, products in store_products.items():
                        for product, count in products.items():
                            combined_products_by_store[store][product] += count
                    for store, total in store_totals.items():
                        combined_stores[store] += total
        return combined_products_by_store, combined_stores

    def top_50_stores_by_total_price(self):
        _, combined_totals = self.combine_results(self.num_processes)
        top_50_stores = sorted(combined_totals.items(), key=lambda x: x[1], reverse=True)[:50]
        output_file = os.path.join(self.output_dir, 'top_50_stores.csv')
        with open(output_file, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['Store Code', 'Total Price'])
            for store, total in top_50_stores:
                csvwriter.writerow([store, total])

    def top_100_products_by_store(self):
        file_size = os.path.getsize(self.filename)
        chunk_size = file_size // self.num_processes

        processes = []
        for i in range(self.num_processes):
            start = i * chunk_size
            end = None if i == self.num_processes - 1 else (i + 1) * chunk_size
            p = multiprocessing.Process(target=process_file_segment,
                                        args=(self.filename, start, end, i, self.temp_dir))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        combined_results, _ = self.combine_results(self.num_processes)

        # Write to CSV for each store
        for store_code, products in combined_results.items():
            top_100_products = sorted(products.items(), key=lambda x: x[1], reverse=True)[:100]
            output_file = os.path.join(self.output_dir, 'top-100-by-store', f'top-100-products-store-{store_code}.csv')
            with open(output_file, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(['Product ID', 'Count'])
                for product, count in top_100_products:
                    csvwriter.writerow([product, count])

    def run_all(self):
        self.top_100_products_by_store()
        self.top_50_stores_by_total_price()


if __name__ == '__main__':
    script_dir = os.path.dirname(__file__)
    data_path = os.path.join(script_dir, '..', 'data', 'filetest.psv')
    result = os.path.join(script_dir, '..', 'multi_process_results')

    processor = DataProcessor(data_path, result)
    processor.run_all()

