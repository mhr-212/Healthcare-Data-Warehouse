"""
Healthcare Data Warehouse - ETL Pipeline
Extracts data from CSV files, anonymizes PII, adds differential privacy, and loads to PostgreSQL
"""

import pandas as pd
import psycopg2
import logging
from hashlib import sha256
import numpy as np
from datetime import datetime
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'dbname': 'health_dw',
    'user': 'user',
    'password': 'pass',
    'host': 'localhost',
    'port': '5433'
}


def anonymize_name(name):
    """Hash patient name using SHA-256 for anonymization"""
    return sha256(name.encode()).hexdigest()


def calculate_age(dob):
    """Calculate age from date of birth"""
    if pd.isna(dob):
        return None
    today = datetime.today()
    dob = pd.to_datetime(dob)
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))


def age_to_group(age):
    """Convert age to age group for privacy"""
    if pd.isna(age):
        return 'Unknown'
    elif age < 18:
        return 'Child (0-17)'
    elif age < 35:
        return 'Young Adult (18-34)'
    elif age < 50:
        return 'Adult (35-49)'
    elif age < 65:
        return 'Middle-Aged (50-64)'
    else:
        return 'Senior (65+)'


def add_differential_privacy_noise(value, epsilon=0.1):
    """Add Laplacian noise for differential privacy"""
    if pd.isna(value):
        return value
    # Laplace mechanism: noise ~ Laplace(0, sensitivity/epsilon)
    sensitivity = 1.0  # Adjust based on your needs
    noise = np.random.laplace(0, sensitivity / epsilon)
    return max(0, value + noise)  # Ensure non-negative for costs


def log_audit(conn, action_type, table_name, record_count, details):
    """Log ETL operations to audit table"""
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO audit_log (action_type, table_name, record_id, user_name, details)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (action_type, table_name, record_count, 'etl_system', details)
        )
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Failed to log audit: {e}")


def load_patients(conn):
    """Load and anonymize patient data"""
    logger.info("Loading patient data...")
    
    try:
        # Read CSV
        df = pd.read_csv('data/synthetic_patients.csv')
        logger.info(f"Read {len(df)} patient records from CSV")
        
        # Anonymize PII
        df['anonymized_name'] = df['patient_name'].apply(anonymize_name)
        
        # Calculate age and age group
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        df['age'] = df['date_of_birth'].apply(calculate_age)
        df['age_group'] = df['age'].apply(age_to_group)
        
        # Data quality check
        df['valid_record'] = df['patient_id'].notna() & df['patient_name'].notna()
        
        # Load to database
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO staging_patients 
                (patient_id, patient_name, anonymized_name, date_of_birth, age, age_group,
                 gender, phone, email, address, city, state, zip_code, valid_record)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (patient_id) DO UPDATE SET
                    patient_name = EXCLUDED.patient_name,
                    anonymized_name = EXCLUDED.anonymized_name,
                    age = EXCLUDED.age,
                    age_group = EXCLUDED.age_group
                """,
                (row['patient_id'], row['patient_name'], row['anonymized_name'],
                 row['date_of_birth'], row['age'], row['age_group'], row['gender'],
                 row['phone'], row['email'], row['address'], row['city'],
                 row['state'], row['zip_code'], row['valid_record'])
            )
        
        conn.commit()
        cursor.close()
        
        logger.info(f"✓ Loaded {len(df)} patient records to staging_patients")
        log_audit(conn, 'LOAD', 'staging_patients', len(df), 
                  f'Loaded {len(df)} anonymized patient records')
        
        return len(df)
        
    except Exception as e:
        logger.error(f"Error loading patients: {e}")
        conn.rollback()
        raise


def load_providers(conn):
    """Load provider data"""
    logger.info("Loading provider data...")
    
    try:
        # Read CSV
        df = pd.read_csv('data/synthetic_providers.csv')
        logger.info(f"Read {len(df)} provider records from CSV")
        
        # Data quality check
        df['valid_record'] = df['provider_id'].notna() & df['provider_name'].notna()
        
        # Load to database
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO staging_providers 
                (provider_id, provider_name, specialty, phone, email, valid_record)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (provider_id) DO UPDATE SET
                    provider_name = EXCLUDED.provider_name,
                    specialty = EXCLUDED.specialty
                """,
                (row['provider_id'], row['provider_name'], row['specialty'],
                 row['phone'], row['email'], row['valid_record'])
            )
        
        conn.commit()
        cursor.close()
        
        logger.info(f"✓ Loaded {len(df)} provider records to staging_providers")
        log_audit(conn, 'LOAD', 'staging_providers', len(df),
                  f'Loaded {len(df)} provider records')
        
        return len(df)
        
    except Exception as e:
        logger.error(f"Error loading providers: {e}")
        conn.rollback()
        raise


def load_visits(conn):
    """Load visit data"""
    logger.info("Loading visit data...")
    
    try:
        # Read CSV
        df = pd.read_csv('data/synthetic_visits.csv')
        logger.info(f"Read {len(df)} visit records from CSV")
        
        # Convert date
        df['visit_date'] = pd.to_datetime(df['visit_date'])
        
        # Data quality check
        df['valid_record'] = (
            df['visit_id'].notna() & 
            df['patient_id'].notna() & 
            df['provider_id'].notna() &
            df['visit_date'].notna()
        )
        
        # Load to database
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO staging_visits 
                (visit_id, patient_id, provider_id, visit_date, visit_type,
                 diagnosis, procedure_performed, cost, valid_record)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (visit_id) DO UPDATE SET
                    visit_date = EXCLUDED.visit_date,
                    cost = EXCLUDED.cost
                """,
                (row['visit_id'], row['patient_id'], row['provider_id'],
                 row['visit_date'], row['visit_type'], row['diagnosis'],
                 row['procedure_performed'], row['cost'], row['valid_record'])
            )
        
        conn.commit()
        cursor.close()
        
        logger.info(f"✓ Loaded {len(df)} visit records to staging_visits")
        log_audit(conn, 'LOAD', 'staging_visits', len(df),
                  f'Loaded {len(df)} visit records')
        
        return len(df)
        
    except Exception as e:
        logger.error(f"Error loading visits: {e}")
        conn.rollback()
        raise


def verify_data(conn):
    """Verify loaded data"""
    logger.info("\nVerifying loaded data...")
    
    cursor = conn.cursor()
    
    tables = ['staging_patients', 'staging_visits', 'staging_providers']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        logger.info(f"  {table}: {count:,} records")
    
    cursor.close()


def main():
    """Main ETL execution"""
    logger.info("="*60)
    logger.info("Healthcare Data Warehouse - ETL Pipeline")
    logger.info("="*60)
    
    conn = None
    
    try:
        # Connect to database
        logger.info("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✓ Connected successfully")
        
        # Load data
        patient_count = load_patients(conn)
        provider_count = load_providers(conn)
        visit_count = load_visits(conn)
        
        # Verify
        verify_data(conn)
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("ETL Summary:")
        logger.info(f"  ✓ Patients loaded: {patient_count:,}")
        logger.info(f"  ✓ Providers loaded: {provider_count:,}")
        logger.info(f"  ✓ Visits loaded: {visit_count:,}")
        logger.info("="*60)
        logger.info("✓ ETL pipeline completed successfully!")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        sys.exit(1)
        
    except FileNotFoundError as e:
        logger.error(f"Data file not found: {e}")
        logger.error("Please run generate_data.py first to create synthetic data")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
        
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()
