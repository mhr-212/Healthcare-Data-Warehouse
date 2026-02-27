-- ============================================
-- Metabase Dashboard Query Pack
-- Healthcare Data Warehouse
-- ============================================

-- 1) KPI - Total Patients
SELECT COUNT(*) AS total_patients
FROM get_dim_patients();

-- 2) KPI - Total Visits
SELECT COUNT(*) AS total_visits
FROM get_fact_visits();

-- 3) KPI - Average Visit Cost (Original vs Privacy)
SELECT
    ROUND(AVG(cost), 2) AS avg_cost_original,
    ROUND(AVG(cost_with_privacy), 2) AS avg_cost_privacy,
    ROUND(ABS(AVG(cost) - AVG(cost_with_privacy)), 2) AS avg_noise
FROM get_fact_visits();

-- 4) Trend - Monthly Visits
SELECT
    DATE_TRUNC('month', visit_date)::DATE AS month,
    COUNT(*) AS visits,
    ROUND(SUM(cost), 2) AS total_cost
FROM get_fact_visits()
GROUP BY DATE_TRUNC('month', visit_date)
ORDER BY month;

-- 5) Demographics - Visits by Age Group
SELECT
    p.age_group,
    COUNT(f.visit_key) AS total_visits,
    ROUND(AVG(f.cost), 2) AS avg_cost
FROM get_dim_patients() p
LEFT JOIN get_fact_visits() f ON p.patient_key = f.patient_key
GROUP BY p.age_group
ORDER BY total_visits DESC;

-- 6) Provider Performance - Visits by Specialty
SELECT
    pr.specialty,
    COUNT(f.visit_key) AS total_visits,
    ROUND(AVG(f.cost), 2) AS avg_cost
FROM get_dim_providers() pr
LEFT JOIN get_fact_visits() f ON pr.provider_key = f.provider_key
GROUP BY pr.specialty
ORDER BY total_visits DESC;

-- 7) Clinical Mix - Top Diagnoses
SELECT
    diagnosis,
    COUNT(*) AS diagnosis_count,
    ROUND(AVG(cost), 2) AS avg_cost
FROM get_fact_visits()
WHERE diagnosis IS NOT NULL
GROUP BY diagnosis
ORDER BY diagnosis_count DESC
LIMIT 10;

-- 8) Compliance - Audit Activity by Day
SELECT
    DATE(action_timestamp) AS action_date,
    action_type,
    table_name,
    COUNT(*) AS event_count
FROM audit_log
GROUP BY DATE(action_timestamp), action_type, table_name
ORDER BY action_date DESC, action_type, table_name;

-- 9) Compliance - Audit Events by User
SELECT
    user_name,
    action_type,
    COUNT(*) AS event_count,
    MIN(action_timestamp) AS first_seen,
    MAX(action_timestamp) AS last_seen
FROM audit_log
GROUP BY user_name, action_type
ORDER BY event_count DESC;
