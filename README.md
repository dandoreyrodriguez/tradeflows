# TradeFlows  

**A trade data pipeline for economic research**

Project TradeFlows a **tooling-first pipeline** for doing **trade analysis**. It is a **scalable system** that allows the user to access graunlar trade data for research and policy. 

For import/export data, the pipeline uses **UN Comtrade** data, via their `comtradeapicall` python package. Users can access the raw data provided by national authorities or work with UN-harmonised data. For  tariff calculations, we assemble national sources.  

---

## Architecture 

It is a **multi-language pipeline**, because good tools matter more than language purity.

### Python: ingestion 
Python handles:
- UN Comtrade API interaction
- bulk downloads (Final and Tariffline)
- availability checks
- raw → parquet conversion
- logging and manifests
- CLI interface using `tradeflows` command

Ingesting the data via a CLI facilitates repeatable, parameterised runs without the need to edit code. `python3` shines here.

### R + DuckDB: analysis without memory pain
R is used for:
- DuckDB-backed querying
- `dbplyr` pipelines
- tidyverse-style cleaning and analysis

The key reason we use R is because it is able to deploy the power of `tidyverse` operations on `duckdb` (database) objects directly, without us having to commit huge objects to memory. `dbply` translates familiar `dplyr` operations to SQL in the background...so we don't have to!

---

## What TradeFlows does today

- Accepts ISO-3 country codes via CLI interface
- Checks data availability before downloading
- Downloads Tariffline (raw) or Final (harmonised) datasets
- Writes Hive-partitioned, split-up parquet files
- Produces structured per-reporter and multi-report summaries
- Logs every action

---

## Directory layout

```text
data/
└── comtrade/
    ├── raw/
    │   └── dataset=Tariffline/
    │       └── type=C/
    │           └── cl=HS/
    │               └── freq=M/
    │                   └── reporter=840/
    │                       └── period=2025-09/
    ├── parquet/
    │   └── dataset=Tariffline/
    │       └── ...
    └── logs/
        └── tradeflows.log
tradeflows.cli/
├── __init__.py
└── tradeflows_cli.py

pyproject.toml
01a_comtrade_datafetch.py
01b_tariffs_datafetch.py
02_dataload.R
README.md
.env                         # where API keys are stored (in .gitignore) 
.gitignore
```
---

## What TradeFlows will do

1. Standardise a pipeline for establishing a range of trade-related stylised facts
2. Deploy a methodolgy for robustly estimating product-level unit value indices (UVIs)
3. Estimate moments which can be used to calibrate structural models.

