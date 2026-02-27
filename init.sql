-- Healthcare Data Warehouse Initialization Script
-- Creates star schema with staging tables, audit logging, and partitioning

-- ============================================
-- STAGING TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS staging_patients (
    patient_id INTEGER PRIMARY KEY,
    patient_name VARCHAR(255),
    anonymized_name VARCHAR(64),
    date_of_birth DATE,
    age INTEGER,
    age_group VARCHAR(20),
    gender VARCHAR(10),
    phone VARCHAR(50),
    email VARCHAR(100),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    valid_record BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_visits (
    visit_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    provider_id INTEGER,
    visit_date DATE,
    visit_type VARCHAR(50),
    diagnosis VARCHAR(255),
    procedure_performed VARCHAR(255),
    cost NUMERIC(10, 2),
    valid_record BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_providers (
    provider_id INTEGER PRIMARY KEY,
    provider_name VARCHAR(255),
    specialty VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(100),
    valid_record BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- DIMENSION TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS dim_patients (
    patient_key SERIAL PRIMARY KEY,
    patient_id INTEGER UNIQUE NOT NULL,
    anonymized_name VARCHAR(64),
    age_group VARCHAR(20),
    gender VARCHAR(10),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_providers (
    provider_key SERIAL PRIMARY KEY,
    provider_id INTEGER UNIQUE NOT NULL,
    provider_name VARCHAR(255),
    specialty VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(100),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- ============================================
-- FACT TABLE (Partitioned by visit_date)
-- ============================================

CREATE TABLE IF NOT EXISTS fact_visits (
    visit_key BIGSERIAL,
    visit_id INTEGER NOT NULL,
    patient_key INTEGER REFERENCES dim_patients(patient_key),
    provider_key INTEGER REFERENCES dim_providers(provider_key),
    visit_date DATE NOT NULL,
    visit_type VARCHAR(50),
    diagnosis VARCHAR(255),
    procedure_performed VARCHAR(255),
    cost NUMERIC(10, 2),
    cost_with_privacy NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (visit_key, visit_date)
) PARTITION BY RANGE (visit_date);

-- Create partitions for different years
CREATE TABLE IF NOT EXISTS fact_visits_2023 PARTITION OF fact_visits
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE IF NOT EXISTS fact_visits_2024 PARTITION OF fact_visits
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS fact_visits_2025 PARTITION OF fact_visits
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS fact_visits_2026 PARTITION OF fact_visits
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

-- ============================================
-- AUDIT LOGGING TABLE (HIPAA Compliance)
-- ============================================

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id BIGSERIAL PRIMARY KEY,
    action_type VARCHAR(50) NOT NULL,
    table_name VARCHAR(100),
    record_id INTEGER,
    user_name VARCHAR(100),
    action_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ip_address VARCHAR(45),
    details TEXT
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

-- Staging table indexes
CREATE INDEX IF NOT EXISTS idx_staging_patients_id ON staging_patients(patient_id);
CREATE INDEX IF NOT EXISTS idx_staging_visits_patient ON staging_visits(patient_id);
CREATE INDEX IF NOT EXISTS idx_staging_visits_provider ON staging_visits(provider_id);
CREATE INDEX IF NOT EXISTS idx_staging_visits_date ON staging_visits(visit_date);

-- Dimension table indexes
CREATE INDEX IF NOT EXISTS idx_dim_patients_id ON dim_patients(patient_id);
CREATE INDEX IF NOT EXISTS idx_dim_patients_age_group ON dim_patients(age_group);
CREATE INDEX IF NOT EXISTS idx_dim_providers_id ON dim_providers(provider_id);
CREATE INDEX IF NOT EXISTS idx_dim_providers_specialty ON dim_providers(specialty);

-- Fact table indexes (on each partition)
CREATE INDEX IF NOT EXISTS idx_fact_visits_patient ON fact_visits(patient_key);
CREATE INDEX IF NOT EXISTS idx_fact_visits_provider ON fact_visits(provider_key);
CREATE INDEX IF NOT EXISTS idx_fact_visits_date ON fact_visits(visit_date);

-- Audit log indexes
CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(action_timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_log_table ON audit_log(table_name);

-- ============================================
-- HELPER FUNCTIONS
-- ============================================

-- Function to log data access
CREATE OR REPLACE FUNCTION log_audit(
    p_action_type VARCHAR,
    p_table_name VARCHAR,
    p_record_id INTEGER,
    p_user_name VARCHAR,
    p_details TEXT
) RETURNS VOID AS $$
BEGIN
    INSERT INTO audit_log (action_type, table_name, record_id, user_name, details)
    VALUES (p_action_type, p_table_name, p_record_id, p_user_name, p_details);
END;
$$ LANGUAGE plpgsql;

-- Audited read helper functions (PostgreSQL does not support SELECT triggers)
CREATE OR REPLACE FUNCTION get_dim_patients()
RETURNS SETOF dim_patients AS $$
BEGIN
    PERFORM log_audit('READ', 'dim_patients', NULL::INTEGER, CURRENT_USER::VARCHAR, 'Audited read via get_dim_patients()');
    RETURN QUERY SELECT * FROM dim_patients;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION get_dim_providers()
RETURNS SETOF dim_providers AS $$
BEGIN
    PERFORM log_audit('READ', 'dim_providers', NULL::INTEGER, CURRENT_USER::VARCHAR, 'Audited read via get_dim_providers()');
    RETURN QUERY SELECT * FROM dim_providers;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION get_fact_visits()
RETURNS SETOF fact_visits AS $$
BEGIN
    PERFORM log_audit('READ', 'fact_visits', NULL::INTEGER, CURRENT_USER::VARCHAR, 'Audited read via get_fact_visits()');
    RETURN QUERY SELECT * FROM fact_visits;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- ============================================
-- GRANTS (for dbt user)
-- ============================================

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "user";
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "user";

-- Success message
DO $$ 
BEGIN
    RAISE NOTICE 'Healthcare Data Warehouse schema initialized successfully!';
END $$;
