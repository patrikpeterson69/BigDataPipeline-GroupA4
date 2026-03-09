# BigDataPipeline - Group A4

## Setup

### Prerequisites
- Python 3.10+
- pip

### Installation

```bash
pip install -r requirements.txt
```

### Data

Datasetet är **NYC FHVHV Trip Data** från NYC Taxi & Limousine Commission och laddas ner automatiskt.

### Kör pipeline

1. Ladda ner data och läs in:
```bash
python src/ingest.py
```

Data laddas ner till `data/` automatiskt. Redan nedladdade filer hoppas över vid omkörning.

2. Transformera data:
```bash
python src/transform.py
```
