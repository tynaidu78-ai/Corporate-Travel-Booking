# Databricks Notebooks

These are sanitized portfolio notebooks for the Corporate Travel Booking Data Platform.

## Execution Order

1. `01_bronze_bookings_raw_refresh.ipynb`
2. `02_bronze_bookings_clean_stage_refresh.ipynb`
3. `03_silver_bookings_validation_refresh.ipynb`
4. `04_gold_bookings_detail_refresh.ipynb`
5. `05_gold_summary_refresh.ipynb`

## Notes

- Replace placeholder storage paths before running.
- Confirm that the Unity Catalog catalog/schemas exist:
  - `corporate_travel_analytics.bronze`
  - `corporate_travel_analytics.silver`
  - `corporate_travel_analytics.gold`
  - `corporate_travel_analytics.control`
- The Silver and Gold incremental notebooks expect a control table named:
  - `corporate_travel_analytics.control.pipeline_watermark`
- Notebook outputs have been cleared before publishing.
- No secrets, keys, tokens, or real customer data are included.
