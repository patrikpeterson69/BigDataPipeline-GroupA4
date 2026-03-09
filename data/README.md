# Data

Raw data is not included in the repository.

## Download

1. Skaffa en Kaggle API-token på https://www.kaggle.com/settings → API → Create New Token
2. Sätt miljövariabeln:
   ```bash
   export KAGGLE_API_TOKEN=<din-token>
   ```
3. Kör nedladdningsskriptet:
   ```bash
   python scripts/download_data.py
   ```

Filerna placeras automatiskt i denna mapp.
