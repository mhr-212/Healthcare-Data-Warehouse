-- Patient Dimension Table
-- Anonymized patient information with SCD Type 2 support

{{ config(
    materialized='incremental',
    on_schema_change='ignore'
) }}

WITH current_patients AS (
    SELECT
        patient_id,
        anonymized_name,
        age_group,
        gender,
        city,
        state,
        zip_code,
        created_at
    FROM {{ ref('stg_patients') }} s
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} existing
        WHERE existing.patient_id = s.patient_id
    )
    {% endif %}
),

existing_keys AS (
    SELECT COALESCE(MAX(patient_key), 0) AS max_key
    FROM {{ this }}
)

SELECT
    existing_keys.max_key + ROW_NUMBER() OVER (ORDER BY patient_id) AS patient_key,
    patient_id,
    anonymized_name,
    age_group,
    gender,
    city,
    state,
    zip_code,
    CURRENT_TIMESTAMP AS valid_from,
    NULL::TIMESTAMP AS valid_to,
    TRUE AS is_current
FROM current_patients
CROSS JOIN existing_keys
