import csv
import os

def export_involve_conversion_to_csv(filepath, data):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    if not data or not isinstance(data, list):
        raise ValueError("Data must be a list of dicts")
    if len(data) == 0:
        return
    with open(filepath, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data) 