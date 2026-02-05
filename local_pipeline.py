import apache_beam as beam
import json
import random
import time
from datetime import datetime, timedelta

# CONFIGURATION
TOTAL_RECORDS = 5000  # Set to 5,000 as requested

class GenerateChaosData(beam.DoFn):
    def process(self, element):
        items = ["Laptop", "Mouse", "Monitor", "Keyboard", "Headset", "Webcam", "Hard Drive"]
        cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Pune", "Kolkata"]
        payments = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash"]
        
        start_time = time.time()
        
        for i in range(TOTAL_RECORDS):
            # 90% Valid Data
            if random.random() > 0.10:
                record = {
                    "order_id": f"ORD-{random.randint(100000, 999999)}",
                    "customer_id": f"CUST-{random.randint(1000, 9999)}",
                    "item_id": random.choice(items),
                    "store_city": random.choice(cities),
                    "payment_method": random.choice(payments),
                    "price": round(random.uniform(10.0, 2000.0), 2),
                    "timestamp": datetime.now().isoformat(),
                    "data_quality_tag": "CLEAN"
                }
            
            # 10% "Chaos" Data (The Trap)
            else:
                issue_type = random.choice(["NEGATIVE_PRICE", "MISSING_ID", "FUTURE_DATE", "NULL_CITY"])
                
                price = round(random.uniform(10.0, 2000.0), 2)
                item = random.choice(items)
                city = random.choice(cities)
                
                if issue_type == "NEGATIVE_PRICE":
                    price = round(random.uniform(-100.0, -1.0), 2)
                elif issue_type == "MISSING_ID":
                    item = None
                elif issue_type == "FUTURE_DATE":
                    # Date in the year 2030
                    timestamp = (datetime.now() + timedelta(days=365*4)).isoformat()
                elif issue_type == "NULL_CITY":
                    city = None

                record = {
                    "order_id": f"ERR-{random.randint(100000, 999999)}",
                    "customer_id": f"CUST-{random.randint(1000, 9999)}",
                    "item_id": item,
                    "store_city": city,
                    "payment_method": random.choice(payments),
                    "price": price,
                    "timestamp": datetime.now().isoformat() if issue_type != "FUTURE_DATE" else timestamp,
                    "data_quality_tag": issue_type
                }
            
            yield record

        print(f">>> Finished generating {TOTAL_RECORDS} rows in {round(time.time() - start_time, 2)} seconds.")

def run():
    print(f">>> Starting Pipeline to generate {TOTAL_RECORDS} records...")
    with beam.Pipeline(runner='DirectRunner') as p:
        (
            p
            | "Start" >> beam.Create([None])
            | "Generate Stream" >> beam.ParDo(GenerateChaosData())
            | "Format JSON" >> beam.Map(json.dumps)
            | "Write to File" >> beam.io.WriteToText('medium_dataset/orders', file_name_suffix='.json')
        )
    print(">>> Pipeline Finished. Check the 'medium_dataset' folder!")

if __name__ == '__main__':
    run()