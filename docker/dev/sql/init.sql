-- ============================================================
-- Voz Company Crawler — PostgreSQL Schema Init
-- ============================================================
-- The 'raw' schema and its tables are created automatically by dlt
-- on the first pipeline run (via dataset_name="raw").
--
-- We only pre-create the dbt transformation layers here so that
-- dbt can write to them without needing CREATE SCHEMA permissions.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;  -- dbt stg_* views
CREATE SCHEMA IF NOT EXISTS marts;    -- dbt dim_* and fct_* tables
