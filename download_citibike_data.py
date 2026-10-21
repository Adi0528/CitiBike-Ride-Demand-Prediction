import os
import zipfile
import requests

BASE_URL = "https://s3.amazonaws.com/tripdata/"
OUTPUT_DIR = "/opt/airflow/data/incoming"

MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]


def try_download(year_month):
    compact = year_month.replace("-", "")
    candidates = [
        f"{compact}-citibike-tripdata.zip",
        f"{compact}-citibike-tripdata.csv.zip",
    ]
    for filename in candidates:
        url = f"{BASE_URL}{filename}"
        resp = requests.get(url, stream=True)
        if resp.status_code == 200:
            zip_path = os.path.join(OUTPUT_DIR, filename)
            with open(zip_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded: {filename}")
            return zip_path
        resp.close()
    print(f"Could not find a file for {year_month}")
    return None


def unzip_csv(zip_path):
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(OUTPUT_DIR)
    print(f"Extracted: {zip_path}")


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for ym in MONTHS:
        zip_path = try_download(ym)
        if zip_path:
            unzip_csv(zip_path)
