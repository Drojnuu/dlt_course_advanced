import dlt
import requests
import time
from datetime import datetime
import os

os.environ["EXTRACT__WORKERS"] = "2"
os.environ["NORMALIZE__WORKERS"] = "2"
os.environ["LOAD__WORKERS"] = "2"


@dlt.source
def jaffle_source(start_date="2017-08-01"):

    @dlt.resource(
        name="orders",
        write_disposition="append",
        primary_key="id",
        parallelized=True
    )
    def orders(start_date=start_date):
        base_url = "https://jaffle-shop.scalevector.ai/api/v1/orders"
        page = 1
        page_size = 100

        while True:
            url = f"{base_url}?page={page}&page_size={page_size}&start_date={start_date}"
            print(f"[{datetime.utcnow()}] Fetching: {url}")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            filtered_data = [r for r in data if float(r.get("order_total", 0)) <= 500]
            yield filtered_data

            if len(data) < page_size:
                break

            last_order = data[-1]
            start_date = last_order.get("ordered_at")
            page += 1

    @dlt.resource(
        name="customers",
        write_disposition="append",
        primary_key="id",
        parallelized=True
    )
    def customers():
        base_url = "https://jaffle-shop.scalevector.ai/api/v1/customers"
        page = 1
        page_size = 100

        while True:
            url = f"{base_url}?page={page}&page_size={page_size}"
            print(f"[{datetime.utcnow()}] Fetching: {url}")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            yield data

            if len(data) < page_size:
                break
            page += 1

    @dlt.resource(
        name="products",
        write_disposition="append",
        primary_key="sku",
        parallelized=True
    )
    def products():
        base_url = "https://jaffle-shop.scalevector.ai/api/v1/products"
        page = 1
        page_size = 100

        while True:
            url = f"{base_url}?page={page}&page_size={page_size}"
            print(f"[{datetime.utcnow()}] Fetching: {url}")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            yield data

            if len(data) < page_size:
                break
            page += 1

    return [orders(), customers(), products()]


# === Pipeline ===

start_time = time.time()
print(f"=== Pipeline started at {datetime.utcnow()} ===")

pipeline = dlt.pipeline(
    pipeline_name="orders_pipeline2",
    destination="duckdb",
    dataset_name="ecommerce_data"
)

stage_start = time.time()


load_info = pipeline.run(jaffle_source())

stage_end = time.time()
print(f"pipeline.run completed in {stage_end - stage_start:.2f} seconds")

end_time = time.time()
print(f"=== Pipeline completed at {datetime.utcnow()} ===")
print(f"Total pipeline time: {end_time - start_time:.2f} seconds")
print("Detailed pipeline trace:")
print(pipeline.last_trace)
