#!/usr/bin/env bash
set -euo pipefail

PROFILE="${1:-fmcg-free}"
CATALOG="${2:-fmcg_dev}"
ROOT="dbfs:/Volumes/${CATALOG}/bronze/landing"

for directory in customers products gross_price orders/landing orders/processed; do
  databricks fs mkdir "$ROOT/$directory" --profile "$PROFILE"
done

databricks fs cp --profile "$PROFILE" --overwrite \
  0_data/2_child_company/full_load/customers/customers.csv \
  "$ROOT/customers/customers.csv"
databricks fs cp --profile "$PROFILE" --overwrite \
  0_data/2_child_company/full_load/products/products.csv \
  "$ROOT/products/products.csv"
databricks fs cp --profile "$PROFILE" --overwrite \
  0_data/2_child_company/full_load/gross_price/gross_price.csv \
  "$ROOT/gross_price/gross_price.csv"
databricks fs cp --profile "$PROFILE" --recursive --overwrite \
  0_data/2_child_company/full_load/orders/landing \
  "$ROOT/orders/landing"

echo "Uploaded demo landing data to /Volumes/${CATALOG}/bronze/landing"
