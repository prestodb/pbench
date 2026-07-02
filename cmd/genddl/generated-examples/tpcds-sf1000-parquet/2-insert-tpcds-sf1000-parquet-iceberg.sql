
INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.call_center SELECT /*+ COALESCE(200) */ * FROM source_schema_name.call_center_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.catalog_page SELECT /*+ COALESCE(200) */ * FROM source_schema_name.catalog_page_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.catalog_returns SELECT /*+ COALESCE(200) */ * FROM source_schema_name.catalog_returns_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.catalog_sales SELECT /*+ COALESCE(2000) */ * FROM source_schema_name.catalog_sales_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.customer SELECT /*+ COALESCE(200) */ * FROM source_schema_name.customer_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.customer_address SELECT /*+ COALESCE(200) */ * FROM source_schema_name.customer_address_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.customer_demographics SELECT /*+ COALESCE(200) */ * FROM source_schema_name.customer_demographics_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.date_dim SELECT /*+ COALESCE(200) */ * FROM source_schema_name.date_dim_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.household_demographics SELECT /*+ COALESCE(200) */ * FROM source_schema_name.household_demographics_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.income_band SELECT /*+ COALESCE(200) */ * FROM source_schema_name.income_band_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.inventory SELECT /*+ COALESCE(2000) */ * FROM source_schema_name.inventory_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.item SELECT /*+ COALESCE(200) */ * FROM source_schema_name.item_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.promotion SELECT /*+ COALESCE(200) */ * FROM source_schema_name.promotion_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.reason SELECT /*+ COALESCE(200) */ * FROM source_schema_name.reason_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.ship_mode SELECT /*+ COALESCE(200) */ * FROM source_schema_name.ship_mode_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.store SELECT /*+ COALESCE(200) */ * FROM source_schema_name.store_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.store_returns SELECT /*+ COALESCE(200) */ * FROM source_schema_name.store_returns_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.store_sales SELECT /*+ COALESCE(2000) */ * FROM source_schema_name.store_sales_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.time_dim SELECT /*+ COALESCE(200) */ * FROM source_schema_name.time_dim_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.warehouse SELECT /*+ COALESCE(200) */ * FROM source_schema_name.warehouse_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.web_page SELECT /*+ COALESCE(200) */ * FROM source_schema_name.web_page_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.web_returns SELECT /*+ COALESCE(200) */ * FROM source_schema_name.web_returns_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.web_sales SELECT /*+ COALESCE(2000) */ * FROM source_schema_name.web_sales_ext;

INSERT OVERWRITE TABLE target_catalog_name.target_schema_name.web_site SELECT /*+ COALESCE(200) */ * FROM source_schema_name.web_site_ext;
