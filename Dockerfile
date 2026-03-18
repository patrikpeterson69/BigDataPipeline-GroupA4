FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    dask[distributed] \
    pandas \
    pyarrow \
    requests \
    "bokeh>=3.1.0"

COPY src/ ./src/

CMD ["python", "src/transformDask.py"]
