-- Provider Dimension Table
-- Healthcare provider information

{{ config(
    materialized='incremental',
    on_schema_change='ignore'
) }}

WITH current_providers AS (
    SELECT
        provider_id,
        provider_name,
        specialty,
        phone,
        email,
        created_at
    FROM {{ ref('stg_providers') }} s
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} existing
        WHERE existing.provider_id = s.provider_id
    )
    {% endif %}
),

existing_keys AS (
    SELECT COALESCE(MAX(provider_key), 0) AS max_key
    FROM {{ this }}
)

SELECT
    existing_keys.max_key + ROW_NUMBER() OVER (ORDER BY provider_id) AS provider_key,
    provider_id,
    provider_name,
    specialty,
    phone,
    email,
    CURRENT_TIMESTAMP AS valid_from,
    NULL::TIMESTAMP AS valid_to,
    TRUE AS is_current
FROM current_providers
CROSS JOIN existing_keys
