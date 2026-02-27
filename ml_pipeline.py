"""
Healthcare Data Warehouse - Machine Learning Models
Predictive analytics for patient readmission, cost forecasting, and anomaly detection

Models:
1. Patient Readmission Risk (Classification)
2. Cost Prediction (Regression)
3. Anomaly Detection (Unsupervised)
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor, IsolationForest
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import classification_report, mean_squared_error, r2_score
import psycopg2
import joblib
import json
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ml_training.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection
def get_connection():
    """Create database connection"""
    return psycopg2.connect(
        host="localhost",
        port=5433,
        database="health_dw",
        user="user",
        password="pass"
    )

# Feature engineering
def engineer_features(conn):
    """
    Create features for ML models from warehouse data
    """
    logger.info("Starting feature engineering...")
    
    query = """
    SELECT 
        p.patient_id,
        p.age_group,
        p.gender,
        p.state,
        pr.specialty,
        f.visit_type,
        f.diagnosis,
        f.procedure_performed,
        f.cost,
        f.visit_date,
        -- Aggregated features
        COUNT(*) OVER (PARTITION BY p.patient_id) as total_visits,
        AVG(f.cost) OVER (PARTITION BY p.patient_id) as avg_patient_cost,
        MAX(f.visit_date) OVER (PARTITION BY p.patient_id) as last_visit_date,
        MIN(f.visit_date) OVER (PARTITION BY p.patient_id) as first_visit_date
    FROM public.fact_visits f
    JOIN public.dim_patients p ON f.patient_key = p.patient_key
    JOIN public.dim_providers pr ON f.provider_key = pr.provider_key
    ORDER BY p.patient_id, f.visit_date
    """
    
    df = pd.read_sql(query, conn)
    logger.info(f"Loaded {len(df)} visit records")
    
    # Calculate days since last visit in pandas
    df['last_visit_date'] = pd.to_datetime(df['last_visit_date'])
    df['days_since_last_visit'] = (pd.Timestamp.now() - df['last_visit_date']).dt.days
    
    # Create readmission target (will patient return within 30 days?)
    df['visit_date'] = pd.to_datetime(df['visit_date'])
    df = df.sort_values(['patient_id', 'visit_date'])
    
    # Calculate days until next visit
    df['next_visit_date'] = df.groupby('patient_id')['visit_date'].shift(-1)
    df['days_to_next_visit'] = (df['next_visit_date'] - df['visit_date']).dt.days
    df['readmitted_30days'] = (df['days_to_next_visit'] <= 30).astype(int)
    
    # Encode categorical variables
    le_age = LabelEncoder()
    le_gender = LabelEncoder()
    le_specialty = LabelEncoder()
    le_visit_type = LabelEncoder()
    le_diagnosis = LabelEncoder()
    
    df['age_group_encoded'] = le_age.fit_transform(df['age_group'])
    df['gender_encoded'] = le_gender.fit_transform(df['gender'])
    df['specialty_encoded'] = le_specialty.fit_transform(df['specialty'])
    df['visit_type_encoded'] = le_visit_type.fit_transform(df['visit_type'])
    df['diagnosis_encoded'] = le_diagnosis.fit_transform(df['diagnosis'])
    
    # Save encoders
    encoders = {
        'age_group': le_age,
        'gender': le_gender,
        'specialty': le_specialty,
        'visit_type': le_visit_type,
        'diagnosis': le_diagnosis
    }
    joblib.dump(encoders, 'ml_models/encoders.pkl')
    logger.info("Saved label encoders")
    
    return df, encoders

# Model 1: Readmission Risk Prediction
def train_readmission_model(df):
    """
    Train RandomForest model to predict 30-day readmission risk
    """
    logger.info("Training readmission prediction model...")
    
    # Remove last visit for each patient (no next visit to predict)
    df_model = df[df['next_visit_date'].notna()].copy()
    
    # Features
    feature_cols = [
        'age_group_encoded', 'gender_encoded', 'specialty_encoded',
        'visit_type_encoded', 'diagnosis_encoded', 'cost',
        'total_visits', 'avg_patient_cost', 'days_since_last_visit'
    ]
    
    X = df_model[feature_cols]
    y = df_model['readmitted_30days']
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # Train model
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        class_weight='balanced'
    )
    
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    logger.info("\nReadmission Model Performance:")
    logger.info(classification_report(y_test, y_pred))
    
    # Feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    logger.info("\nFeature Importance:")
    logger.info(feature_importance.to_string())
    
    # Save model
    joblib.dump(model, 'ml_models/readmission_model.pkl')
    logger.info("Saved readmission model")
    
    return model, feature_importance

# Model 2: Cost Prediction
def train_cost_model(df):
    """
    Train RandomForest regression model to predict visit costs
    """
    logger.info("Training cost prediction model...")
    
    # Features
    feature_cols = [
        'age_group_encoded', 'gender_encoded', 'specialty_encoded',
        'visit_type_encoded', 'diagnosis_encoded',
        'total_visits', 'avg_patient_cost'
    ]
    
    X = df[feature_cols]
    y = df['cost']
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Train model
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        random_state=42
    )
    
    model.fit(X_train, y_train)
    
    # Evaluate
    y_pred = model.predict(X_test)
    
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)
    
    logger.info("\nCost Prediction Model Performance:")
    logger.info(f"RMSE: ${rmse:.2f}")
    logger.info(f"R² Score: {r2:.4f}")
    
    # Feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    logger.info("\nFeature Importance:")
    logger.info(feature_importance.to_string())
    
    # Save model
    joblib.dump(model, 'ml_models/cost_model.pkl')
    logger.info("Saved cost prediction model")
    
    return model, feature_importance

# Model 3: Anomaly Detection
def train_anomaly_model(df):
    """
    Train Isolation Forest for anomaly detection
    """
    logger.info("Training anomaly detection model...")
    
    # Features
    feature_cols = [
        'cost', 'total_visits', 'avg_patient_cost', 'days_since_last_visit'
    ]
    
    X = df[feature_cols].dropna()
    
    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Train model
    model = IsolationForest(
        contamination=0.05,  # Expect 5% anomalies
        random_state=42
    )
    
    model.fit(X_scaled)
    
    # Predict anomalies
    predictions = model.predict(X_scaled)
    anomaly_scores = model.score_samples(X_scaled)
    
    # -1 for anomalies, 1 for normal
    n_anomalies = (predictions == -1).sum()
    
    logger.info(f"\nAnomalies detected: {n_anomalies} ({n_anomalies/len(X)*100:.2f}%)")
    
    # Save model and scaler
    joblib.dump(model, 'ml_models/anomaly_model.pkl')
    joblib.dump(scaler, 'ml_models/anomaly_scaler.pkl')
    logger.info("Saved anomaly detection model")
    
    return model, scaler

# Generate predictions for all patients
def generate_predictions(df, readmission_model, cost_model, anomaly_model, scaler, encoders):
    """
    Generate predictions for all records and save to database
    """
    logger.info("Generating predictions for all records...")
    
    # Readmission risk features
    readmission_features = [
        'age_group_encoded', 'gender_encoded', 'specialty_encoded',
        'visit_type_encoded', 'diagnosis_encoded', 'cost',
        'total_visits', 'avg_patient_cost', 'days_since_last_visit'
    ]
    
    # Cost prediction features
    cost_features = [
        'age_group_encoded', 'gender_encoded', 'specialty_encoded',
        'visit_type_encoded', 'diagnosis_encoded',
        'total_visits', 'avg_patient_cost'
    ]
    
    # Anomaly detection features
    anomaly_features = [
        'cost', 'total_visits', 'avg_patient_cost', 'days_since_last_visit'
    ]
    
    # Generate predictions
    df_pred = df.copy()
    
    # Readmission risk (for records with next visit data)
    valid_idx = df_pred['next_visit_date'].notna()
    if valid_idx.any():
        df_pred.loc[valid_idx, 'readmission_risk'] = readmission_model.predict_proba(
            df_pred.loc[valid_idx, readmission_features]
        )[:, 1]
    
    # Cost prediction
    df_pred['predicted_cost'] = cost_model.predict(df_pred[cost_features])
    
    # Anomaly detection
    anomaly_data = df_pred[anomaly_features].dropna()
    anomaly_scaled = scaler.transform(anomaly_data)
    anomaly_preds = anomaly_model.predict(anomaly_scaled)
    df_pred.loc[anomaly_data.index, 'is_anomaly'] = (anomaly_preds == -1).astype(int)
    df_pred.loc[anomaly_data.index, 'anomaly_score'] = anomaly_model.score_samples(anomaly_scaled)
    
    logger.info(f"Generated predictions for {len(df_pred)} records")
    
    return df_pred

# Save predictions to database
def save_predictions_to_db(df_pred, conn):
    """
    Save ML predictions to database
    """
    logger.info("Saving predictions to database...")
    
    # Create predictions table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ml_predictions (
        patient_id INTEGER,
        visit_date DATE,
        readmission_risk FLOAT,
        predicted_cost FLOAT,
        actual_cost FLOAT,
        is_anomaly INTEGER,
        anomaly_score FLOAT,
        prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (patient_id, visit_date)
    );
    """
    
    cursor = conn.cursor()
    cursor.execute(create_table_query)
    conn.commit()
    
    # Insert predictions
    insert_query = """
    INSERT INTO ml_predictions 
    (patient_id, visit_date, readmission_risk, predicted_cost, actual_cost, is_anomaly, anomaly_score)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (patient_id, visit_date) DO UPDATE SET
        readmission_risk = EXCLUDED.readmission_risk,
        predicted_cost = EXCLUDED.predicted_cost,
        actual_cost = EXCLUDED.actual_cost,
        is_anomaly = EXCLUDED.is_anomaly,
        anomaly_score = EXCLUDED.anomaly_score,
        prediction_date = CURRENT_TIMESTAMP;
    """
    
    # Prepare data
    records = []
    for _, row in df_pred.iterrows():
        records.append((
            int(row['patient_id']),
            row['visit_date'].date(),
            float(row.get('readmission_risk', 0)) if pd.notna(row.get('readmission_risk')) else 0,
            float(row['predicted_cost']),
            float(row['cost']),
            int(row.get('is_anomaly', 0)) if pd.notna(row.get('is_anomaly')) else 0,
            float(row.get('anomaly_score', 0)) if pd.notna(row.get('anomaly_score')) else 0
        ))
    
    cursor.executemany(insert_query, records)
    conn.commit()
    
    logger.info(f"Saved {len(records)} predictions to database")
    cursor.close()

# Main training pipeline
def main():
    """
    Main ML training pipeline
    """
    logger.info("=" * 80)
    logger.info("Starting ML Model Training Pipeline")
    logger.info("=" * 80)
    
    # Create models directory
    import os
    os.makedirs('ml_models', exist_ok=True)
    
    # Connect to database
    conn = get_connection()
    
    try:
        # Feature engineering
        df, encoders = engineer_features(conn)
        
        # Train models
        readmission_model, readmission_importance = train_readmission_model(df)
        cost_model, cost_importance = train_cost_model(df)
        anomaly_model, scaler = train_anomaly_model(df)
        
        # Generate predictions
        df_pred = generate_predictions(df, readmission_model, cost_model, anomaly_model, scaler, encoders)
        
        # Save to database
        save_predictions_to_db(df_pred, conn)
        
        # Save metadata
        metadata = {
            'training_date': datetime.now().isoformat(),
            'total_records': len(df),
            'readmission_model': 'RandomForestClassifier',
            'cost_model': 'RandomForestRegressor',
            'anomaly_model': 'IsolationForest',
            'features': {
                'readmission': list(readmission_importance['feature']),
                'cost': list(cost_importance['feature'])
            }
        }
        
        with open('ml_models/metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info("\n" + "=" * 80)
        logger.info("ML Model Training Complete!")
        logger.info("=" * 80)
        logger.info("Models saved to ml_models/")
        logger.info("Predictions saved to database table: ml_predictions")
        
    except Exception as e:
        logger.error(f"Error in ML pipeline: {e}", exc_info=True)
        raise
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
