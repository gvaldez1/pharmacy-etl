# Pharmacy Metrics and Recommendations

## Overview

This project processes pharmacy claims data to calculate performance metrics, recommend the most cost-efficient pharmacy chains per drug, and identify common prescription patterns to support better business decisions.

## Description

This project simulates a pharmacy claims processing system where patients purchase generic and branded medications at the lowest possible prices, with or without insurance.

The workflow involves prescriptions being filled by pharmacies, claims being submitted, and in some cases reverted when consumers do not complete the purchase.

## Goals

The goal of this project is to process raw data stored in JSON files and extract insights that support business decision-making. Specifically, the project focuses on:

- **Data Extraction**:** Read pharmacy, claims, and reverts datasets from the data/ folder, with emphasis on pharmacy events
- **Metrics & Performance Analysis**: Calculate and analyze key metrics such as the average unit price offered by pharmacies, enabling identification of outliers and new opportunities
- **Recommendations**: Provide the top 2 pharmacy chains per drug, ranked by the lowest average unit price, to help highlight cost-efficient options
- **Prescription Patterns**: Identify the most common prescribed quantities for each drug, which will assist the business team in negotiating better price discounts.

By combining data exploration, metrics computation, and recommendation logic, this project aims to generate actionable insights that directly support cost optimization strategies and strengthen partnerships with pharmacy chains.

## Stacks and packages

- Python v3.12.11
- PySpark v3.5.0
- orjson v3.9.15

## Project structure

```
pharmacy-etl/
├─ data/                # input files (claims, reverts, pharmacies)
├─ output/              # output files
├─ spark_jobs/          # PySpark job for recommendations and drug most common quantity prescribed
├─ src/data_etl/        # Python code for load inputfiles and to compute metrics
├─ app.py               # main entrypoint with argparse
└─ requirements.txt
```

## Inputs

All provided files should be placed inside the data/ folder:

- Pharmacies: CSV with id,chain
- Claims: JSON (array or NDJSON) with id, npi, ndc, price, quantity, timestamp
- Reverts: JSON (array or NDJSON) with id, claim_id, timestamp

> We are only considering claims where npi exists in `pharmacies` dataset.

## Getting Started

### Prerequisites

- Python 3.12
- openjdk@17

------

### Clone the repository

```code
git clone https://github.com/gvaldez1/pharmacy-etl.git
```
Go to the project folder

------

### Environment Variables

Create a virtual environment and install dependencies:

**Unix/macOS**

```code
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

**Windows**

```code
py -m venv .venv
.venv\Scripts\activate
pip3 install -r src/requirements.txt
```
------

### Execution

**1. Metrics per npi and ndc (Python)**

Go to your terminal and run the following command in the project root:

```bash
python app.py metrics \
  --pharmacies data/pharmacies \
  --claims data/claims \
  --reverts data/reverts \
  --output output/metrics_by_npi_ndc.json
```

**Output**:
- `output/metrics_by_npi_ndc.json`

Example:

```json
[
  {
    "npi": "0000000000",
    "ndc": "00002323401",
    "fills": 82,
    "reverted": 4,
    "avg_price": 377.56,
    "total_price": 2509345.2
  }
]
```
------

**2. Make a recommendation for the top 2 chain to be displayed for each drug and return the most common quantity prescribed for a given drug (PySpark)**

Go to your terminal and run the following command in the project root:

```bash
python app.py spark \
  --pharmacies data/pharmacies/output-09482089-7f1b-4d36-a21f-4652ed460166.csv \
  --claims data/claims \
  --reverts data/reverts \
  --output-top2-chain output/top2_chain_per_ndc.json \
  --output-most-common-qty output/most_common_qty_per_ndc.json
```

**Output**:
- `output/top2_chain_per_ndc.json` → top 2 chains with lowest avg unit price per *drug*
- `output/most_common_qty_per_ndc.json` → top 5 most common prescribed quantities per *drug*

## Next Steps

- Add unit tests for metric calculations
- Containerize the application with Docker
- Build a simple dashboard for results visualization