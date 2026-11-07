# Databricks Retail Lakehouse

This project demonstrates a simple end-to-end Databricks Lakehouse pipeline using the Medallion Architecture.

The goal is to build a small portfolio project that shows practical Databricks fundamentals, including data ingestion, Delta Lake, Unity Catalog, SQL transformations, and table optimization.

## Architecture

Source Data
↓
Bronze Layer
↓
Silver Layer
↓
Gold Layer
↓
SQL Analytics

## Project Objectives

This project will demonstrate how to:

- Ingest retail data into Databricks
- Create Bronze, Silver, and Gold Delta tables
- Clean and transform data using SQL and PySpark
- Use Delta Lake operations such as MERGE
- Work with Unity Catalog
- Inspect Delta table history
- Use OPTIMIZE and basic performance techniques
- Query business-ready Gold tables using Databricks SQL

## Medallion Architecture

### Bronze

The Bronze layer stores raw or minimally transformed source data.

Purpose:

- Preserve source data
- Support auditability
- Allow reprocessing if required

### Silver

The Silver layer contains cleaned and standardized data.

Typical transformations include:

- Data type conversion
- Deduplication
- Null handling
- Data quality checks
- Standardization
- Joins

### Gold

The Gold layer contains business-ready datasets.

Examples:

- Monthly sales
- Customer metrics
- Product performance
- Revenue summaries

## Technologies

- Databricks
- Apache Spark
- PySpark
- Databricks SQL
- Delta Lake
- Unity Catalog
- GitHub

## Repository Structure

```text
databricks-retail-lakehouse/
│
├── README.md
│
├── notebooks/
│   ├── 01_bronze_ingestion
│   ├── 02_silver_transformation
│   ├── 03_gold_aggregation
│   └── 04_data_analysis
│
├── sql/
│   └── analysis_queries.sql
│
└── docs/
    └── architecture.md
