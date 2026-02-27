"""
Healthcare Data Warehouse - Synthetic Data Generator
Generates 10,000 patient records, 50 providers, and 25,000+ visit records
"""

import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# Constants
NUM_PATIENTS = 10000
NUM_PROVIDERS = 50
AVG_VISITS_PER_PATIENT = 2.5

# Medical data
SPECIALTIES = [
    'Cardiology', 'Dermatology', 'Emergency Medicine', 'Family Medicine',
    'Internal Medicine', 'Neurology', 'Obstetrics', 'Oncology',
    'Orthopedics', 'Pediatrics', 'Psychiatry', 'Radiology', 'Surgery'
]

DIAGNOSES = [
    'Hypertension', 'Type 2 Diabetes', 'Upper Respiratory Infection',
    'Anxiety Disorder', 'Depression', 'Back Pain', 'Arthritis',
    'Migraine', 'Asthma', 'COPD', 'Hyperlipidemia', 'Coronary Artery Disease',
    'Gastroesophageal Reflux', 'Urinary Tract Infection', 'Pneumonia',
    'Allergic Rhinitis', 'Osteoporosis', 'Hypothyroidism'
]

PROCEDURES = [
    'Physical Examination', 'Blood Test', 'X-Ray', 'CT Scan', 'MRI',
    'Ultrasound', 'ECG', 'Vaccination', 'Minor Surgery', 'Consultation',
    'Prescription Refill', 'Physical Therapy', 'Endoscopy', 'Biopsy'
]

VISIT_TYPES = ['Routine Checkup', 'Follow-up', 'Emergency', 'Consultation', 'Surgery']


def generate_patients():
    """Generate synthetic patient data"""
    logger.info(f"Generating {NUM_PATIENTS} patient records...")
    
    patients = []
    for i in range(1, NUM_PATIENTS + 1):
        patient = {
            'patient_id': i,
            'patient_name': fake.name(),
            'date_of_birth': fake.date_of_birth(minimum_age=1, maximum_age=90),
            'gender': random.choice(['Male', 'Female', 'Other']),
            'phone': fake.phone_number(),
            'email': fake.email(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode()
        }
        patients.append(patient)
    
    df = pd.DataFrame(patients)
    df.to_csv('data/synthetic_patients.csv', index=False)
    logger.info(f"✓ Generated {len(df)} patient records")
    return df


def generate_providers():
    """Generate synthetic provider data"""
    logger.info(f"Generating {NUM_PROVIDERS} provider records...")
    
    providers = []
    for i in range(1, NUM_PROVIDERS + 1):
        provider = {
            'provider_id': i,
            'provider_name': f"Dr. {fake.name()}",
            'specialty': random.choice(SPECIALTIES),
            'phone': fake.phone_number(),
            'email': fake.email()
        }
        providers.append(provider)
    
    df = pd.DataFrame(providers)
    df.to_csv('data/synthetic_providers.csv', index=False)
    logger.info(f"✓ Generated {len(df)} provider records")
    return df


def generate_visits(num_patients, num_providers):
    """Generate synthetic visit data"""
    num_visits = int(NUM_PATIENTS * AVG_VISITS_PER_PATIENT)
    logger.info(f"Generating ~{num_visits} visit records...")
    
    visits = []
    visit_id = 1
    
    # Generate dates over the past 3 years
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2026, 2, 15)
    
    for patient_id in range(1, num_patients + 1):
        # Each patient has 1-5 visits
        num_patient_visits = random.randint(1, 5)
        
        for _ in range(num_patient_visits):
            visit_date = fake.date_between(start_date=start_date, end_date=end_date)
            
            visit = {
                'visit_id': visit_id,
                'patient_id': patient_id,
                'provider_id': random.randint(1, num_providers),
                'visit_date': visit_date,
                'visit_type': random.choice(VISIT_TYPES),
                'diagnosis': random.choice(DIAGNOSES),
                'procedure_performed': random.choice(PROCEDURES),
                'cost': round(random.uniform(100, 5000), 2)
            }
            visits.append(visit)
            visit_id += 1
    
    df = pd.DataFrame(visits)
    df = df.sort_values('visit_date')
    df.to_csv('data/synthetic_visits.csv', index=False)
    logger.info(f"✓ Generated {len(df)} visit records")
    return df


def main():
    """Main execution function"""
    logger.info("="*60)
    logger.info("Healthcare Data Warehouse - Synthetic Data Generation")
    logger.info("="*60)
    
    try:
        # Generate data
        patients_df = generate_patients()
        providers_df = generate_providers()
        visits_df = generate_visits(NUM_PATIENTS, NUM_PROVIDERS)
        
        # Summary statistics
        logger.info("\n" + "="*60)
        logger.info("Generation Summary:")
        logger.info(f"  Patients: {len(patients_df):,}")
        logger.info(f"  Providers: {len(providers_df):,}")
        logger.info(f"  Visits: {len(visits_df):,}")
        logger.info(f"  Avg visits per patient: {len(visits_df)/len(patients_df):.2f}")
        logger.info(f"  Date range: {visits_df['visit_date'].min()} to {visits_df['visit_date'].max()}")
        logger.info("="*60)
        logger.info("✓ All synthetic data generated successfully!")
        
    except Exception as e:
        logger.error(f"Error generating data: {e}")
        raise


if __name__ == "__main__":
    main()
