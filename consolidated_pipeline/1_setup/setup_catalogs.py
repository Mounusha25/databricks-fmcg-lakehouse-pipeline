# Databricks notebook source
"""
Catalog and Schema Setup

Purpose:
- Initialize Unity Catalog structure for FMCG data warehouse
- Create Bronze, Silver, and Gold schemas following medallion architecture
- Ensure idempotent catalog and schema creation
- Establish namespace foundation for all downstream pipelines

Tech:
- Databricks Unity Catalog
- SQL DDL
- Databricks Jobs compatible
"""

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg;
# MAGIC USE CATALOG fmcg;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.bronze;