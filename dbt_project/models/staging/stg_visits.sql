-- Staging model for visits
-- Validates visit records before transformation

{{ config(materialized='view') }}

SELECT
    visit_id,
    patient_id,
    provider_id,
    visit_date,
    visit_type,
    diagnosis,
    procedure_performed,
    cost,
    valid_record,
    created_at
FROM staging_visits
WHERE valid_record = TRUE
    AND visit_id IS NOT NULL
    AND patient_id IS NOT NULL
    AND provider_id IS NOT NULL
    AND visit_date IS NOT NULL
    AND visit_date <= CURRENT_DATE  -- No future dates
