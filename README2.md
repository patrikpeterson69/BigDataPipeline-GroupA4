# BigDataPipeline - Group A4

NYC Taxi pipeline som kör lokalt och skalar till Azure Databricks utan kodändringar.

---

## Arkitektur

```
┌─────────────────────────────────────────────────────────┐
│                   KONFIGURATIONS-LAGER                  │
│  config.py — PIPELINE_ENV=local | databricks            │
│  Byter automatiskt sökvägar, credentials och session    │
└───────────────────┬─────────────────────────────────────┘
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
┌───────────────┐       ┌───────────────────────────┐
│  LOKALT       │       │  AZURE (DATABRICKS)        │
│               │       │                            │
│  data/*.parq  │       │  wasbs://data@group4...    │
│  (12 GB lokal)│       │  Azure Blob Storage        │
└───────┬───────┘       └────────────┬───────────────┘
        │                            │
        ▼                            ▼
┌─────────────────────────────────────────────────────────┐
│                   BERÄKNINGS-LAGER                      │
│                                                         │
│  transformSparkCloud.py   transformDaskCloud.py         │
│  ┌─────────────────────┐  ┌──────────────────────┐     │
│  │ PySpark (lokal mode)│  │ Dask (lokal)         │     │
│  │ 1 driver, 4 cores   │  │ multi-thread, pandas │     │
│  └─────────────────────┘  └──────────────────────┘     │
│              ↕ (samma kod, annan session)               │
│  ┌─────────────────────┐                               │
│  │ Databricks Cluster  │                               │
│  │ 1 driver + N workers│                               │
│  │ Skalas automatiskt  │                               │
│  └─────────────────────┘                               │
└───────────────────┬─────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────┐
│                   RESULTAT-LAGER                        │
│  data/processed/  (lokalt)                              │
│  wasbs://.../processed  (Databricks → ADLS)             │
└─────────────────────────────────────────────────────────┘
```

### Vad som förändras i molnet

| Komponent          | Lokalt                        | Azure Databricks                        |
|--------------------|-------------------------------|-----------------------------------------|
| **Data**           | `data/*.parquet`              | `wasbs://data@group4datalake.blob...`   |
| **SparkSession**   | Skapas med `builder`          | `getActiveSession()` (kluster-session)  |
| **Skrivning**      | `toPandas().to_parquet()`     | `df.write.parquet()` direkt till ADLS   |
| **Skalning**       | 1 maskin, begränsat RAM       | Kluster med autoscale                   |
| **Aktivering**     | `PIPELINE_ENV=local` (default)| `PIPELINE_ENV=databricks`               |

### Designbeslut för skalbarhet

- **`broadcast(zones)`** — den lilla zontabellen (265 rader) broadcastas till alla workers, eliminerar shuffle-join som annars kostar O(n) nätverkstrafik
- **`dropna(subset=[...])`** — pekar ut specifika kolumner så Spark kan göra column pruning vid läsning av parquet (läser bara relevanta kolumner)
- **`spark.sql.shuffle.partitions=200`** — konfigurerbart, standard för kluster med 200 cores; lokalt kan sänkas
- **`getActiveSession()`** — återanvänder Databricks inbyggda session istället för att starta ny, nödvändigt för cluster-mode
- **Config-abstraktion** — all miljöspecifik logik i `config.py`, pipelines är miljöagnostiska

---

## Kom igång (lokalt)

### 1. Klona repot
```bash
git clone <repo-url>
cd BigDataPipeline-GroupA4
```

### 2. Skapa och aktivera virtual environment
```bash
python -m venv .venv

# Windows Git Bash / Mac / Linux:
source .venv/Scripts/activate

# Windows PowerShell:
.venv\Scripts\Activate.ps1
```

### 3. Installera dependencies
```bash
pip install -r requirements.txt
```

### 4. Ladda ner Hadoop winutils (endast Windows)

PySpark kräver `winutils.exe` för att fungera på Windows:

```powershell
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe" -OutFile "bin/winutils.exe"
```

### 5. Ladda ner data (~12 GB)
```bash
python src/ingest.py
```

### 6. Kör pipeline

**PySpark (lokal):**
```bash
python src/transformSparkCloud.py
```

**Dask (lokal):**
```bash
python src/transformDaskCloud.py
```

---

## Köra mot Azure Databricks

### Steg 1 — Konfigurera kluster
I Databricks UI under Cluster → Advanced → Spark Config, lägg till:
```
fs.azure.account.key.group4datalake.blob.core.windows.net <ACCESS_KEY>
```

### Steg 2 — Skapa notebook
1. Workspace → Create → Notebook → Python
2. Välj ert kluster
3. Lägg till i första cellen:
```python
import os
os.environ["PIPELINE_ENV"] = "databricks"
```
4. Klistra in innehållet från `src/transformSparkCloud.py`
5. Kör → Run All

### Steg 3 — Alternativt: kör lokalt mot Azure
```powershell
$env:PIPELINE_ENV="databricks"
$env:AZURE_STORAGE_KEY="<din_nyckel>"
python src/transformSparkCloud.py
```

---

## Transformationer

Fyra operationer körs på ~50 miljoner resor (12 månaders FHVHV-data):

| Steg | Operation | Spark API |
|------|-----------|-----------|
| 1 | Filtrering (pris > 0, inga nullvärden) | `filter()`, `dropna()` |
| 2 | Join med taxizoner (pickup + dropoff) | `join()` med `broadcast()` |
| 3 | Aggregation per borough (antal + snittpris) | `groupBy().agg()` |
| 4 | Window function: top-3 zoner per borough | `Window.partitionBy().orderBy()` + `rank()` |

---

## Projektstruktur

```
src/
  ingest.py              # Laddar ner parquet-filer från NYC TLC
  config.py              # Miljökonfiguration (local/databricks) — ej i git
  transformSpark.py      # Lokal PySpark-pipeline
  transformSparkCloud.py # Cloud-ready PySpark (lokal + Databricks)
  transformDask.py       # Lokal Dask-pipeline
  transformDaskCloud.py  # Cloud-ready Dask
  utils.py               # Loggning
data/
  *.parquet              # Rådata (ignoreras av git)
  taxi_zone_lookup.csv   # Zonreferens (laddas ner automatiskt)
  processed/             # Resultat (ignoreras av git)
bin/
  winutils.exe           # Hadoop Windows-binär (ignoreras av git)
```
