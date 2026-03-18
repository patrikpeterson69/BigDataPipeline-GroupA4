# Dask Transformation – Körinstruktioner

Pipeline som läser 34 parquet-filer med NYC taxi-data, aggregerar per borough och rankar topp-3 zoner per borough.

## Krav

- Python 3.11+
- Docker Desktop (igång)
- Minikube
- kubectl

## Alternativ 1: Kör lokalt

```powershell
# Aktivera virtual environment
.venv\Scripts\Activate.ps1

# Kör skriptet
python src/transformDask.py
```

Dask dashboard: [http://localhost:8787/status](http://localhost:8787/status)

## Alternativ 2: Kör via Minikube (Kubernetes)

### Steg 1 – Starta Minikube

```powershell
minikube start --cpus=4 --memory=8192
```

### Steg 2 – Sätt Docker-kontexten till Minikube

```powershell
minikube docker-env | Invoke-Expression
```

### Steg 3 – Montera datafolder

Kör i ett **separat terminalfönster** och håll det öppet under hela körningen:

```powershell
minikube mount C:/Users/<användarnamn>/Documents/GitHub/BigDataPipeline-GroupA4/data:/mnt/data
```

### Steg 4 – Bygg Docker-imagen

```powershell
docker build -t dask-transform:latest .
```

### Steg 5 – Kör jobbet

```powershell
kubectl apply -f k8s/dask-job.yaml
kubectl logs -f job/dask-transform
```

### Steg 6 – (Valfritt) Dask dashboard

Kör i ett separat fönster medan jobbet körs:

```powershell
kubectl port-forward job/dask-transform 8787:8787
```

Öppna [http://localhost:8787/status](http://localhost:8787/status)

### Steg 7 – Rensa upp

```powershell
kubectl delete job dask-transform
minikube stop
```

## Resultat

Outputfiler sparas i `data/processed/`:
- `agg_per_borough_dask.parquet` – antal resor och snittpris per borough
- `ranked_zones_dask.parquet` – topp-3 zoner per borough

## Konfiguration (transformDask.py)

| Parameter | Värde | Förklaring |
|---|---|---|
| `n_workers` | 8 | Antal Dask-workers |
| `threads_per_worker` | 4 | Trådar per worker |
| `processes` | False | Alla workers i samma process |
| `memory_limit` | 0 | Ingen artificiell minnesgräns per worker |
