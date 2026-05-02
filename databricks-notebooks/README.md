# Databricks Notebooks

This folder contains sanitized Databricks notebooks for the Corporate Travel Booking Data Platform.

## Execution Order

1. `01_bronze_bookings_raw_refresh.ipynb`
2. `02_bronze_bookings_clean_stage_refresh.ipynb`
3. `03_silver_bookings_validation_refresh.ipynb`
4. `04_gold_bookings_detail_refresh.ipynb`
5. `05_gold_summary_refresh.ipynb`

## Notes

- These notebooks are sanitized portfolio versions.
- Resource names, paths, and sample values are anonymized.
- Replace placeholder paths before running in a real Databricks workspace.
- Confirm that Unity Catalog schemas exist before execution.
- Notebook outputs have been cleared before publishing.
