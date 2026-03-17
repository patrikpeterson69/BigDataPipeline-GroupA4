import os
from pathlib import Path

# Sätt PIPELINE_ENV=databricks för att köra mot Azure
ENV = os.getenv("PIPELINE_ENV", "local")

if ENV == "databricks":
    STORAGE_ACCOUNT = "group4datalake"
    CONTAINER       = "data"
    ACCESS_KEY      = os.getenv("AZURE_STORAGE_KEY", "")  # TODO: ta bort innan push
    _BASE           = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"
    RAW_PATH        = _BASE
    PROCESSED_PATH  = f"{_BASE}/processed"
    ZONE_FILE_PATH  = f"{_BASE}/taxi_zone_lookup.csv"
else:
    _BASE_DIR      = Path(__file__).parent.parent
    _DATA_DIR      = _BASE_DIR / "data"
    RAW_PATH       = str(_DATA_DIR)
    PROCESSED_PATH = str(_DATA_DIR / "processed")
    ZONE_FILE_PATH = str(_DATA_DIR / "taxi_zone_lookup.csv")
    ACCESS_KEY     = ""
