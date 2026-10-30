# DeliverIQ — Delivery Intelligence

DeliverIQ is a Streamlit dashboard for exploring delivery performance. It
calculates late/on-time status with Apache Spark, displays order and city
metrics, and includes a Hadoop Streaming MapReduce example.

## Project files

- `app.py` — Streamlit dashboard that reads `delivery_data.csv`.
- `delivery_data.csv` — 500 sample delivery orders.
- `mapper.py` and `reducer.py` — Hadoop Streaming mapper and reducer that
  count delayed and on-time orders.
- `mapreduce_output.txt` — MapReduce output displayed by the dashboard.
- `generate_dataset.py` — regenerates the sample CSV dataset.

## Requirements

- Python 3
- Java (required by PySpark)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install streamlit pandas plotly pyspark
```

On Windows PowerShell, activate the environment with `.venv\Scripts\Activate.ps1`.

## Run the dashboard

Run from this directory so the app can find `delivery_data.csv`:

```bash
streamlit run app.py
```

Streamlit opens the dashboard in your browser, usually at
`http://localhost:8501`. Stop the server with `Ctrl+C`.

## Regenerate the sample data

```bash
python generate_dataset.py
```

This overwrites `delivery_data.csv` with 500 reproducible sample orders.

## Run the MapReduce example locally

The mapper and reducer can run as a local pipeline without a Hadoop
installation:

```bash
python mapper.py < delivery_data.csv | sort | python reducer.py > mapreduce_output.txt
```

The dashboard's Hadoop section reads `mapreduce_output.txt`.
