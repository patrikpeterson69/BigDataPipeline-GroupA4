# transformDask — Dask-pipeline på Kubernetes (Minikube)

Denna guide beskriver hur man kör `transformDask.py` i en lokal Kubernetes-miljö med Minikube.

## Vad gör pipelinen?

`src/transformDask.py` läser NYC FHVHV-taxidata i Parquet-format och utför tre steg:

1. **Inläsning** — läser alla `fhvhv_tripdata_*.parquet`-filer parallellt med Dask
2. **Rensning** — tar bort rader med null-värden i `base_passenger_fare`, `PULocationID`, `DOLocationID` samt filtrerar bort negativa/noll-priser
3. **Aggregering per borough** — räknar antal resor och snittpriser per stadsdelskategori
4. **Window function / rankning** — rankar de 3 populäraste pickup-zonerna per borough
5. **Sparar resultat** till `data/processed/` som Parquet-filer

Utdatafiler:
- `data/processed/agg_per_borough_dask.parquet`
- `data/processed/ranked_zones_dask.parquet`

---

## Förutsättningar

Installera följande verktyg innan du börjar:

| Verktyg | Version | Länk |
|---------|---------|------|
| Docker Desktop | >= 24 | https://www.docker.com/products/docker-desktop |
| Minikube | >= 1.32 | https://minikube.sigs.k8s.io/docs/start |
| kubectl | >= 1.28 | https://kubernetes.io/docs/tasks/tools |

> **Windows:** Se till att Hyper-V eller WSL 2 är aktiverat. Minikube fungerar bäst med Docker-drivern på Windows.

---

## Steg-för-steg-guide

### 1. Starta Minikube

```bash
minikube start --driver=docker --memory=16g --cpus=4
```

> Pipelinen kräver minst 10 GB RAM (se `k8s/dask-job.yaml`). Sätt `--memory` till minst `16g` för att ge utrymme åt operativsystemet.

Verifiera att klustret är igång:

```bash
kubectl cluster-info
minikube status
```

---

### 2. Montera datamappen i Minikube

Pipelinen läser data från `/mnt/data` inuti Minikube-noden. Kör detta kommando i en **separat terminal** och låt det köra hela sessionen:

```bash
minikube mount "C:/Users/<DittAnvändarnamn>/Documents/GitHub/BigDataPipeline-GroupA4/data":/mnt/data
```

> Ersätt `<DittAnvändarnamn>` med ditt Windows-användarnamn.
> Terminalen med `minikube mount` måste vara öppen så länge jobbet körs.

Verifiera att monteringen fungerar:

```bash
minikube ssh "ls /mnt/data"
```

Du ska se dina Parquet-filer listade, t.ex. `fhvhv_tripdata_2024-01.parquet`.

---

### 3. Bygg Docker-imagen inuti Minikube

För att Minikube ska kunna använda en lokalt byggd image utan att hämta från Docker Hub behöver du bygga direkt i Minikubes Docker-daemon:

```bash
# Rikta din terminalsession mot Minikubes Docker
eval $(minikube docker-env)

# Bygg imagen (kör från projektets rotkatalog)
docker build -t dask-transform:latest .
```

> **Windows PowerShell:** Använd istället:
> ```powershell
> & minikube -p minikube docker-env --shell powershell | Invoke-Expression
> docker build -t dask-transform:latest .
> ```

Verifiera att imagen finns:

```bash
docker images | grep dask-transform
```

---

### 4. Kör Kubernetes-jobbet

```bash
kubectl apply -f k8s/dask-job.yaml
```

Kontrollera att jobbet startats:

```bash
kubectl get jobs
kubectl get pods
```

---

### 5. Följ loggarna

```bash
# Hämta pod-namn
kubectl get pods

# Streama loggar (byt ut <pod-namn>)
kubectl logs -f <pod-namn>
```

Du ska se timing-output liknande:

```
[TIMING] Inläsning: 12.34s
[TIMING] Filtrering + dropna: 0.01s
[TIMING] Aggregation per borough: 45.67s
[TIMING] Window function: 38.22s
[TIMING] Spara till parquet: 0.45s
[TIMING] Total pipeline: 96.70s
Pipeline färdig!
```

---

### 6. Hämta resultaten

Resultaten sparas direkt till din lokala datamapp via monteringen:

```
data/processed/agg_per_borough_dask.parquet
data/processed/ranked_zones_dask.parquet
```

Du kan läsa dem lokalt med Python:

```python
import pandas as pd
df = pd.read_parquet("data/processed/agg_per_borough_dask.parquet")
print(df)
```

---

## Rensa upp efter körning

Ta bort jobbet efter att det är klart:

```bash
kubectl delete job dask-transform
```

Stoppa Minikube:

```bash
minikube stop
```

---

## Felsökning

### Jobbet fastnar i `Pending`

```bash
kubectl describe pod <pod-namn>
```

Vanliga orsaker:
- Inte tillräckligt med RAM/CPU i Minikube — öka `--memory` och `--cpus` vid `minikube start`
- Imagen hittas inte — kontrollera att du körde `docker build` med `minikube docker-env` aktiverat (`imagePullPolicy: Never` kräver lokal image)

### `Out of Memory` / pod kraschar

Öka Minikubes minne:

```bash
minikube stop
minikube start --driver=docker --memory=20g --cpus=6
```

### Monteringen fungerar inte / filer saknas

- Kontrollera att terminalen med `minikube mount` fortfarande körs
- Kontrollera sökvägen till `data`-mappen — den måste innehålla `fhvhv_tripdata_*.parquet`-filer

### Imagen verkar gammal trots ny `docker build`

```bash
eval $(minikube docker-env)
docker build --no-cache -t dask-transform:latest .
```

---

## Projektstruktur (relevant för denna pipeline)

```
BigDataPipeline-GroupA4/
├── src/
│   ├── transformDask.py      # Huvudpipelinen (denna guide)
│   └── utils.py              # Logger-hjälpfunktion
├── k8s/
│   └── dask-job.yaml         # Kubernetes Job-manifest
├── Dockerfile                # Bygger dask-transform:latest
└── data/
    ├── fhvhv_tripdata_*.parquet   # Indata (läggs hit manuellt)
    └── processed/                 # Utdata skapas här automatiskt
```
