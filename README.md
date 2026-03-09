# BigDataPipeline - Group A4

## Setup

### Prerequisites
- Python 3.10+
- pip
- Ett Kaggle-konto (för att ladda ner data)

### Installation

```bash
pip install -r requirements.txt
```

### Data

Datasetet är **NYC FHVHV Data** från Kaggle och ingår inte i repot.

Kör nedladdningsskriptet:
```bash
python scripts/download_data.py
```

Kagglehub cachar datan lokalt, så den laddas bara ner första gången.

### Running the pipeline

```bash
python src/ingest.py
python src/transform.py
```
