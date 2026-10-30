
import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)

cities = ["Mysuru", "Bengaluru", "Mandya"]
methods = ["Standard", "Express"]

records = []
start_date = datetime(2026, 10, 20)

for i in range(500):
    expected = start_date + timedelta(days=random.randint(0, 30))

    delay = random.choices(
        [-2, -1, 0, 1, 2, 3, 4, 5],
        weights=[5, 10, 25, 20, 15, 12, 8, 5]
    )[0]

    actual = expected + timedelta(days=delay)

    records.append({
        "order_id": 101 + i,
        "city": random.choice(cities),
        "shipping_method": random.choice(methods),
        "expected_date": expected.strftime("%Y-%m-%d"),
        "actual_date": actual.strftime("%Y-%m-%d")
    })

df = pd.DataFrame(records)

df.to_csv("delivery_data.csv", index=False)

print("Dataset generated successfully!")
print("Total orders:", len(df))
print(df.head())
