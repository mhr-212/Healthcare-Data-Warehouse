-- ============================================
-- Healthcare Data Warehouse - Analytics Queries
-- Sample queries for reporting and analysis
-- ============================================

-- 1. Average Visits per Age Group
-- Shows patient visit patterns by age demographics
SELECT 
    p.age_group,
    COUNT(DISTINCT p.patient_key) AS total_patients,
    COUNT(f.visit_key) AS total_visits,
    ROUND(COUNT(f.visit_key)::NUMERIC / COUNT(DISTINCT p.patient_key), 2) AS avg_visits_per_patient,
    ROUND(AVG(f.cost), 2) AS avg_cost_per_visit
FROM get_dim_patients() p
LEFT JOIN get_fact_visits() f ON p.patient_key = f.patient_key
GROUP BY p.age_group
ORDER BY avg_visits_per_patient DESC;

-- 2. Most Common Diagnoses
-- Top 10 diagnoses across all visits
SELECT 
    diagnosis,
    COUNT(*) AS diagnosis_count,
    ROUND(COUNT(*)::NUMERIC * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(cost), 2) AS avg_cost
FROM get_fact_visits()
WHERE diagnosis IS NOT NULL
GROUP BY diagnosis
ORDER BY diagnosis_count DESC
LIMIT 10;

-- 3. Provider Utilization by Specialty
-- Shows workload distribution across provider specialties
SELECT 
    pr.specialty,
    COUNT(DISTINCT pr.provider_key) AS total_providers,
    COUNT(f.visit_key) AS total_visits,
    ROUND(COUNT(f.visit_key)::NUMERIC / COUNT(DISTINCT pr.provider_key), 2) AS avg_visits_per_provider,
    ROUND(AVG(f.cost), 2) AS avg_cost_per_visit
FROM get_dim_providers() pr
LEFT JOIN get_fact_visits() f ON pr.provider_key = f.provider_key
GROUP BY pr.specialty
ORDER BY total_visits DESC;

-- 4. Monthly Visit Trends
-- Time series analysis of visit volume
SELECT 
    DATE_TRUNC('month', visit_date) AS month,
    COUNT(*) AS visit_count,
    COUNT(DISTINCT patient_key) AS unique_patients,
    ROUND(SUM(cost), 2) AS total_revenue,
    ROUND(AVG(cost), 2) AS avg_cost
FROM get_fact_visits()
GROUP BY DATE_TRUNC('month', visit_date)
ORDER BY month;

-- 5. Visit Type Distribution
-- Breakdown of visit types
SELECT 
    visit_type,
    COUNT(*) AS visit_count,
    ROUND(COUNT(*)::NUMERIC * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
    ROUND(AVG(cost), 2) AS avg_cost,
    ROUND(MIN(cost), 2) AS min_cost,
    ROUND(MAX(cost), 2) AS max_cost
FROM get_fact_visits()
GROUP BY visit_type
ORDER BY visit_count DESC;

-- 6. Geographic Distribution of Patients
-- Patient distribution by state
SELECT 
    state,
    COUNT(DISTINCT patient_key) AS patient_count,
    ROUND(COUNT(DISTINCT patient_key)::NUMERIC * 100.0 / SUM(COUNT(DISTINCT patient_key)) OVER (), 2) AS percentage
FROM get_dim_patients()
GROUP BY state
ORDER BY patient_count DESC
LIMIT 15;

-- 7. High-Cost Visits Analysis
-- Identifies visits above 90th percentile cost
WITH cost_percentiles AS (
    SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY cost) AS p90_cost
    FROM get_fact_visits()
)
SELECT 
    f.visit_id,
    p.age_group,
    pr.specialty,
    f.visit_type,
    f.diagnosis,
    f.procedure_performed,
    ROUND(f.cost, 2) AS cost,
    f.visit_date
FROM get_fact_visits() f
JOIN get_dim_patients() p ON f.patient_key = p.patient_key
JOIN get_dim_providers() pr ON f.provider_key = pr.provider_key
CROSS JOIN cost_percentiles
WHERE f.cost > cost_percentiles.p90_cost
ORDER BY f.cost DESC
LIMIT 20;

-- 8. Patient Visit Frequency Cohorts
-- Categorizes patients by visit frequency
WITH patient_visit_counts AS (
    SELECT 
        patient_key,
        COUNT(*) AS visit_count
    FROM get_fact_visits()
    GROUP BY patient_key
)
SELECT 
    CASE 
        WHEN visit_count = 1 THEN '1 visit'
        WHEN visit_count BETWEEN 2 AND 3 THEN '2-3 visits'
        WHEN visit_count BETWEEN 4 AND 5 THEN '4-5 visits'
        ELSE '6+ visits'
    END AS visit_frequency,
    COUNT(*) AS patient_count,
    ROUND(COUNT(*)::NUMERIC * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM patient_visit_counts
GROUP BY 
    CASE 
        WHEN visit_count = 1 THEN '1 visit'
        WHEN visit_count BETWEEN 2 AND 3 THEN '2-3 visits'
        WHEN visit_count BETWEEN 4 AND 5 THEN '4-5 visits'
        ELSE '6+ visits'
    END
ORDER BY patient_count DESC;

-- 9. Procedure Effectiveness by Diagnosis
-- Links procedures to diagnoses
SELECT 
    diagnosis,
    procedure_performed,
    COUNT(*) AS procedure_count,
    ROUND(AVG(cost), 2) AS avg_cost
FROM get_fact_visits()
WHERE diagnosis IS NOT NULL AND procedure_performed IS NOT NULL
GROUP BY diagnosis, procedure_performed
HAVING COUNT(*) >= 5  -- Only show common combinations
ORDER BY diagnosis, procedure_count DESC;

-- 10. Audit Log Query - Data Access Monitoring
-- HIPAA compliance: track data access
SELECT 
    action_type,
    table_name,
    COUNT(*) AS action_count,
    MIN(action_timestamp) AS first_access,
    MAX(action_timestamp) AS last_access,
    user_name
FROM audit_log
GROUP BY action_type, table_name, user_name
ORDER BY last_access DESC;

-- 11. Privacy Comparison - Differential Privacy Impact
-- Compares original vs privacy-enhanced costs
SELECT 
    AVG(cost) AS original_avg_cost,
    AVG(cost_with_privacy) AS privacy_avg_cost,
    ABS(AVG(cost) - AVG(cost_with_privacy)) AS avg_noise,
    ROUND((ABS(AVG(cost) - AVG(cost_with_privacy)) / AVG(cost) * 100), 2) AS noise_percentage
FROM get_fact_visits();

-- 12. Data Quality Check
-- Validates data completeness and integrity
SELECT 
    'Total Patients' AS metric,
    COUNT(*) AS count,
    NULL AS percentage
FROM get_dim_patients()
UNION ALL
SELECT 
    'Total Providers',
    COUNT(*),
    NULL
FROM get_dim_providers()
UNION ALL
SELECT 
    'Total Visits',
    COUNT(*),
    NULL
FROM get_fact_visits()
UNION ALL
SELECT 
    'Visits with Missing Diagnosis',
    COUNT(*),
    ROUND(COUNT(*)::NUMERIC * 100.0 / (SELECT COUNT(*) FROM get_fact_visits()), 2)
FROM get_fact_visits()
WHERE diagnosis IS NULL
UNION ALL
SELECT 
    'Visits with Missing Procedure',
    COUNT(*),
    ROUND(COUNT(*)::NUMERIC * 100.0 / (SELECT COUNT(*) FROM get_fact_visits()), 2)
FROM get_fact_visits()
WHERE procedure_performed IS NULL;
