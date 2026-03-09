"""Download NYC FHVHV dataset from Kaggle using kagglehub."""

import os
import kagglehub
import pandas as pd

DATASET = "jeffsinsel/nyc-fhvhv-data"


def main():
    print("Laddar ner datasetet från Kaggle (detta kan ta lite tid)...")
    # Kagglehub cachar datan, så den laddas bara ner första gången
    dataset_path = kagglehub.dataset_download(DATASET)
    print(f"Dataset nedladdat/hittat på: {dataset_path}\n")

    filer = os.listdir(dataset_path)
    print("Hittade följande filer (visar upp till 5 stycken):")
    for fil in filer[:5]:
        print(f" - {fil}")

    parquet_filer = [f for f in filer if f.endswith(".parquet")]
    if parquet_filer:
        vald_fil = parquet_filer[0]
        komplett_sökväg = os.path.join(dataset_path, vald_fil)
        print(f"\nLäser in data från {vald_fil}...")
        df = pd.read_parquet(komplett_sökväg)
        print("\nFörsta 5 raderna:")
        print(df.head())
    else:
        print("Hittade inga Parquet-filer i datasetet.")


if __name__ == "__main__":
    main()
