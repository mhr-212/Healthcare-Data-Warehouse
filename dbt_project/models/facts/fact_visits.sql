-- Visit Fact Table
-- Contains visit transactions with differential privacy

{{ config(
    materialized='incremental',
    on_schema_change='ignore'
) }}

WITH visits AS (
    SELECT
        v.visit_id,
        v.patient_id,
        v.provider_id,
        v.visit_date,
        v.visit_type,
        v.diagnosis,
        v.procedure_performed,
        v.cost,
        v.created_at
    FROM {{ ref('stg_visits') }} v
    {% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} existing
        WHERE existing.visit_id = v.visit_id
    )
    {% endif %}
),

visits_with_keys AS (
    SELECT
        v.visit_id,
        p.patient_key,
        pr.provider_key,
        v.visit_date,
        v.visit_type,
        v.diagnosis,
        v.procedure_performed,
        v.cost,
        -- Add Laplace noise in SQL (epsilon = 0.1, sensitivity = 1.0)
        GREATEST(
            0,
            v.cost + (
                -(1.0 / 0.1) *
                CASE
                    WHEN (RANDOM() - 0.5) < 0 THEN -1
                    ELSE 1
                END *
                LN(1 - 2 * ABS(RANDOM() - 0.5))
            )
        ) AS cost_with_privacy,
        v.created_at
    FROM visits v
    INNER JOIN {{ ref('dim_patients') }} p ON v.patient_id = p.patient_id
    INNER JOIN {{ ref('dim_providers') }} pr ON v.provider_id = pr.provider_id
),

existing_keys AS (
    SELECT COALESCE(MAX(visit_key), 0) AS max_key
    FROM {{ this }}
)

SELECT
    existing_keys.max_key + ROW_NUMBER() OVER (ORDER BY visit_date, visit_id) AS visit_key,
    visit_id,
    patient_key,
    provider_key,
    visit_date,
    visit_type,
    diagnosis,
    procedure_performed,
    cost,
    cost_with_privacy,
    created_at
FROM visits_with_keys
CROSS JOIN existing_keys
