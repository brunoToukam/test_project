import csv
import os
from collections import defaultdict, Counter
from datetime import timedelta
from timeit import default_timer as timer

# cols
# client_id|ticket_id|product_id|store_code|date|price
start = timer()


script_dir = os.path.dirname(__file__)
data_path = os.path.join(script_dir, '..', 'data', 'filetest.psv')
output_dir = os.path.join(script_dir, '..', 'results')
# filename = 'data/randomized-transactions-202009.psv'

# Initialize a dictionary to store product sales
product_sales = defaultdict(float)
products_by_store = defaultdict(Counter)

# Read data and process line by line
with open(data_path, 'r') as file:
    next(file)
    for i, line in enumerate(file):
        parts = line.strip().split('|')
        if len(parts) == 6 and parts[2]:
            store_code = parts[3]
            product_id = parts[2]
            price = float(parts[5])

        product_sales[store_code] += price
        products_by_store[store_code][product_id] += 1

# Find the most 50 sold products
top_50_stores = sorted(product_sales, key=product_sales.get, reverse=True)[:50]

with open(f'{output_dir}/top-50-stores.csv', 'w') as top_stores:
    wr = csv.writer(top_stores)
    wr.writerow(('store_code', 'total_ca'))
    for product in top_50_stores:
        wr.writerow((product, product_sales[product]))


for store, product_counts in products_by_store.items():
    with open(f'{output_dir}/top-products-by-store/top-100-products-store-{store}.csv', 'w') as top_products_stores:
        wr = csv.writer(top_products_stores)
        wr.writerow(('product_id', 'number_of_products'))

        # Get the top 100 most common products
        top_products = product_counts.most_common(100)

        for product_id, count in top_products:
            wr.writerow((product_id, count))


print('*********', i)

end = timer()

print(timedelta(seconds=end - start))

