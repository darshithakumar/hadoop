
import sys
import csv

reader = csv.DictReader(sys.stdin)

for row in reader:
    expected = row["expected_date"].strip()
    actual = row["actual_date"].strip()

    if actual > expected:
        print("Delayed\t1")
    else:
        print("On Time\t1")
