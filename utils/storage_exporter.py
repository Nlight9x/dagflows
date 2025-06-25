import httpx
import math
import asyncio
import csv
import os
import time


class StorageExporter:
    def __init__(self, **config):
        pass

    def export(self, data, **settings):
        pass


class NocodbExporter(StorageExporter):
    def __init__(self, **config):
        super().__init__()
        self._api_url = config.get('api_url')
        self._token = config.get('token')

    def export(self, data, **settings):
        batch_size = settings.get('batch_size', 1000)
        headers = {
            "accept": "application/json",
            "xc-token": self._token,
            "Content-Type": "application/json"
        }
        responses = []
        total = len(data)
        for i in range(0, total, batch_size):
            batch = data[i:i+batch_size]
            retry = 0
            while retry < 5:
                try:
                    resp = httpx.post(self._api_url, headers=headers, json=batch, timeout=30.0)
                    resp.raise_for_status()
                    responses.append(resp.json())
                    break  # Thành công thì thoát vòng lặp retry
                except Exception as e:
                    retry += 1
                    if retry < 5:
                        print(f"Retry Nocodb export, attempt {retry+1}/5...")
                        time.sleep(10)
                    else:
                        print(f"Nocodb export failed after 5 attempts: {e}")
                        raise
        return responses


class CsvExporter:
    def __init__(self, **config):
        super().__init__()
        self._filepath = config.get('filepath')

    def export(self, data, **settings):
        os.makedirs(os.path.dirname(self._filepath), exist_ok=True)
        if not data or not isinstance(data, list):
            raise ValueError("Data must be a list of dicts")
        if len(data) == 0:
            return
        with open(self._filepath, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
