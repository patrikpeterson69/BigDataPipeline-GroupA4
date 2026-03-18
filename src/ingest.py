"""Data ingestion module - laddar ner NYC FHVHV data från NYC TLC."""

import requests
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
MONTHS = [
    "2020-01", "2020-02", "2020-03", "2020-04", "2020-05", "2020-06",
    "2020-07", "2020-08", "2020-09", "2020-10", "2020-11", "2020-12",
    "2021-01", "2021-02", "2021-03", "2021-04", "2021-05", "2021-06",
    "2021-07", "2021-08", "2021-09", "2021-10", "2021-11", "2021-12",
    "2022-01", "2022-02", "2022-03", "2022-04", "2022-05", "2022-06",
    "2022-07", "2022-08", "2022-09", "2022-10", "2022-11", "2022-12",
]


def ingest():
    DATA_DIR.mkdir(exist_ok=True)

    zone_filename = "taxi_zone_lookup.csv"
    zone_dest = DATA_DIR / zone_filename
    if not zone_dest.exists():
        print(f"Laddar ner {zone_filename}...")
        response = requests.get(ZONE_URL)
        response.raise_for_status()
        zone_dest.write_bytes(response.content)
        print(f"Sparad till {zone_dest}")
    else:
        print(f"{zone_filename} finns redan, hoppar över nedladdning.")

    for month in MONTHS:
        filename = f"fhvhv_tripdata_{month}.parquet"
        dest = DATA_DIR / filename

        if not dest.exists():
            url = f"{BASE_URL}/{filename}"
            print(f"Laddar ner {filename}...")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Sparad till {dest}")
        else:
            print(f"{filename} finns redan, hoppar över nedladdning.")

    files = sorted(DATA_DIR.glob("fhvhv_tripdata_*.parquet"))
    print(f"\nKlart! {len(files)} filer i {DATA_DIR}")
    return [str(f) for f in files]


if __name__ == "__main__":
    ingest()
