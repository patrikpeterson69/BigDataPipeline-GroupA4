# BigDataPipeline - Group A4

## Kom igång (steg för steg)

### 1. Klona repot
```bash
git clone <repo-url>
cd BigDataPipeline-GroupA4
```

### 2. Skapa virtual environment
```bash
python -m venv .venv
```

### 3. Aktivera virtual environment
```bash
# Windows Git Bash / Mac / Linux:
source .venv/Scripts/activate

# Windows PowerShell:
.venv\Scripts\Activate.ps1
```

### 4. Installera dependencies
```bash
pip install -r requirements.txt
```

### 5. Ladda ner Hadoop winutils (endast Windows)

PySpark kräver `winutils.exe` och `hadoop.dll` för att fungera på Windows. Kör i PowerShell:

```powershell
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe" -OutFile "bin/winutils.exe"
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll" -OutFile "bin/hadoop.dll"
```

### 6. Ladda ner data (~12GB)
```bash
python src/ingest.py
```
Filerna laddas ner automatiskt till `data/`. Redan nedladdade filer hoppas över vid omkörning.

### 7. Kör transformationen
```bash
python src/transform.py
```

## Projektstruktur
```
src/
  ingest.py      # Laddar ner parquet-filer från NYC TLC
  transform.py   # Join, aggregation och window functions med PySpark
data/
  *.parquet      # Rådatan (ignoreras av git)
  taxi_zone_lookup.csv  # Zonreferens (laddas ner automatiskt)
```
