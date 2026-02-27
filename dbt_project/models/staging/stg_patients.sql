-- Staging model for patients
-- Performs initial data quality checks and validation

{{ config(materialized='view') }}

SELECT
    patient_id,
    patient_name,
    anonymized_name,
    date_of_birth,
    age,
    age_group,
    gender,
    phone,
    email,
    address,
    city,
    state,
    zip_code,
    valid_record,
    created_at
FROM staging_patients
WHERE valid_record = TRUE
    AND patient_id IS NOT NULL
    AND anonymized_name IS NOT NULL
    AND age_group IS NOT NULL
