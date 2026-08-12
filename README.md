# TradeFlows  

**A trade data pipeline for economic research.**

Project TradeFlows is a **data pipeline** for ingesting and maintaining granular trade data obtained via UN Comtrade. The user must have a premium subscription to Comtrade.

The pipeline uses the `comtradeapicall` python package. Users can access the raw data provided by national authorities or work with UN-harmonised data.

---

## Architecture

Project `tradeflows` has three key principles: 

1. Download only new data (i.e. newly published or revised data) and update the data lake
2. Use Hive partitioning to save the data to facilitate `duckdb`
3. Create and document tiers of cleaning, beginning with raw unedited data.

### Ingestion 

Python handles:
- UN Comtrade API interaction
- bulk downloads (Final and Tariffline)
- availability checks
- raw → parquet conversion
- logging and manifests
- CLI interface using `tradeflows` command


