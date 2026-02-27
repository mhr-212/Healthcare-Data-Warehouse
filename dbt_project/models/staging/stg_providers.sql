-- Staging model for providers
-- Validates provider records

{{ config(materialized='view') }}

SELECT
    provider_id,
    provider_name,
    specialty,
    phone,
    email,
    valid_record,
    created_at
FROM staging_providers
WHERE valid_record = TRUE
    AND provider_id IS NOT NULL
    AND provider_name IS NOT NULL
    AND specialty IS NOT NULL
