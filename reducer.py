
import sys

current_status = None
count = 0

for line in sys.stdin:
    status, value = line.strip().split("\t")
    value = int(value)

    if status == current_status:
        count += value
    else:
        if current_status is not None:
            print(f"{current_status}\t{count}")

        current_status = status
        count = value

if current_status is not None:
    print(f"{current_status}\t{count}")
