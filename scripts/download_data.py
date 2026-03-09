"""Download NYC FHVHV dataset from Kaggle."""

import os
import subprocess
import sys
from pathlib import Path

DATASET = "jeffsinsel/nyc-fhvhv-data"
DATA_DIR = Path(__file__).parent.parent / "data"


def main():
    if not os.environ.get("KAGGLE_API_TOKEN"):
        print("Error: KAGGLE_API_TOKEN environment variable is not set.")
        print("Get your token at https://www.kaggle.com/settings -> API -> Create New Token")
        sys.exit(1)

    DATA_DIR.mkdir(exist_ok=True)

    print(f"Downloading {DATASET} to {DATA_DIR}...")
    result = subprocess.run(
        [sys.executable, "-c",
         f"from kaggle.api.kaggle_api_extended import KaggleApiClient; "
         f"from kaggle import api; api.authenticate(); "
         f"api.dataset_download_files('{DATASET}', path='{DATA_DIR}', unzip=True)"],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        # Fallback: try kaggle CLI
        scripts_dir = Path(sys.executable).parent / "Scripts"
        kaggle_exe = scripts_dir / "kaggle"
        result = subprocess.run(
            [str(kaggle_exe), "datasets", "download", DATASET,
             "--path", str(DATA_DIR), "--unzip"],
            capture_output=True, text=True
        )

    if result.returncode == 0:
        print("Download complete.")
    else:
        print("Error:", result.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
