# Market Pulse

Market Pulse is an end-to-end data engineering and analytics project focused on
ingesting, processing, and analyzing intraday market data (stocks, ETFs).
Data is taken from Alpha Vantage API (1min granularity).
The platform is designed around a cloud-native Bronze / Silver / Gold data lake
architecture on AWS, with a strong emphasis on reliability, reproducibility,
and analytical flexibility.

## High-level architecture

- **Ingest (Bronze)**  
  Event-driven AWS Lambda workers fetch intraday OHLCV market data from an
  external data provider and store raw, immutable datasets in Amazon S3.

- **Silver**  
  AWS Glue (Spark) jobs normalize, validate, deduplicate, and partition
  intraday datasets, preparing them for downstream analytical workloads.

- **Gold**  
  Independent Glue workflows build higher-level aggregates, statistical features,
  and derived datasets optimized for analytical queries and signal research.

- **Analytics**  
  Amazon Athena and Amazon QuickSight are used for exploratory analysis and BI,
  with dbt models providing a semantic and metrics layer on top of curated data.

## Analytical goals

Beyond building a reliable data ingestion and processing platform, the long-term
objective of the project is to enable quantitative research and exploratory
analysis focused on market regime behavior, including:

- Identification of potential **bubble formation signals**
- Detection of early **crash or regime-shift indicators**
- Systematic analysis of potentially **overvalued and undervalued symbols**

The project does not aim at direct price prediction. Instead, it focuses on
constructing robust, interpretable indicators and heuristic signals that can
support market structure and regime analysis. These analytical layers will be
developed incrementally on top of the Gold data layer and are considered future
work once the core data platform is stable.

## Key characteristics

- Event-driven orchestration using EventBridge and Step Functions
- Idempotent, rerun-safe data pipelines
- Clear separation between ingestion, transformation, and analytics layers
- Modular and recomputable Gold layer
- Data layouts optimized for Athena and BI consumption
- Infrastructure defined as code using Terraform

## Repository structure

- `infra/` – Infrastructure as code and cloud resources (Terraform)
- `src/` – Data ingestion, transformation, and analytics code
- `configs/` – Pipeline and ingestion configuration
- `assets/` – Reference and static datasets

## Status

This project is in early development. Core infrastructure and data ingestion
pipelines are currently being implemented.