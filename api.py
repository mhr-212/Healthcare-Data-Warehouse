"""
Healthcare Data Warehouse - REST API
FastAPI application providing access to analytics and predictions

Endpoints:
- /health - Health check
- /analytics/kpis - Key performance indicators
- /analytics/age-groups - Age group analysis
- /analytics/diagnoses - Diagnosis distribution
- /analytics/providers - Provider utilization
- /patients/{id}/visits - Patient visit history (anonymized)
- /predictions/{patient_id} - ML predictions
- /auth/token - JWT authentication
"""

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
from typing import List, Optional
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Configuration
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "health_dw",
    "user": "user",
    "password": "pass"
}

# Initialize FastAPI
app = FastAPI(
    title="Healthcare Data Warehouse API",
    description="RESTful API for healthcare analytics and predictions",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/token")

# Pydantic models
class Token(BaseModel):
    access_token: str
    token_type: str

class User(BaseModel):
    username: str

class KPIResponse(BaseModel):
    total_patients: int
    total_visits: int
    total_providers: int
    avg_cost: float

class AgeGroupData(BaseModel):
    age_group: str
    visit_count: int
    unique_patients: int
    avg_cost: float

class DiagnosisData(BaseModel):
    diagnosis: str
    count: int
    percentage: float

class ProviderData(BaseModel):
    specialty: str
    visits: int
    providers: int
    avg_cost: float

class Visit(BaseModel):
    visit_id: int
    visit_date: str
    visit_type: str
    diagnosis: str
    cost: float
    provider_specialty: str

class Prediction(BaseModel):
    patient_id: int
    visit_date: str
    readmission_risk: Optional[float]
    predicted_cost: float
    actual_cost: float
    is_anomaly: bool

# Database connection
def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

# Authentication functions
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        return User(username=username)
    except JWTError:
        raise credentials_exception

# Endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Healthcare Data Warehouse API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.post("/auth/token", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Authenticate and get access token
    Default credentials: demo/demo123
    """
    # Simple authentication (replace with database lookup in production)
    if form_data.username == "demo" and form_data.password == "demo123":
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": form_data.username}, expires_delta=access_token_expires
        )
        return {"access_token": access_token, "token_type": "bearer"}
    
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Incorrect username or password",
        headers={"WWW-Authenticate": "Bearer"}
    )

@app.get("/analytics/kpis", response_model=KPIResponse)
async def get_kpis(current_user: User = Depends(get_current_user)):
    """Get key performance indicators"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        (SELECT COUNT(DISTINCT patient_id) FROM public.dim_patients) as total_patients,
        (SELECT COUNT(*) FROM public.fact_visits) as total_visits,
        (SELECT COUNT(DISTINCT provider_id) FROM public.dim_providers) as total_providers,
        (SELECT ROUND(AVG(cost), 2) FROM public.fact_visits) as avg_cost
    """
    
    cursor.execute(query)
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    return KPIResponse(**result)

@app.get("/analytics/age-groups", response_model=List[AgeGroupData])
async def get_age_groups(current_user: User = Depends(get_current_user)):
    """Get age group analysis"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        p.age_group,
        COUNT(*) as visit_count,
        COUNT(DISTINCT p.patient_id) as unique_patients,
        ROUND(AVG(f.cost), 2) as avg_cost
    FROM public.fact_visits f
    JOIN public.dim_patients p ON f.patient_key = p.patient_key
    GROUP BY p.age_group
    ORDER BY visit_count DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return [AgeGroupData(**row) for row in results]

@app.get("/analytics/diagnoses", response_model=List[DiagnosisData])
async def get_diagnoses(limit: int = 15, current_user: User = Depends(get_current_user)):
    """Get diagnosis distribution"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        diagnosis,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM public.fact_visits), 2) as percentage
    FROM public.fact_visits
    GROUP BY diagnosis
    ORDER BY count DESC
    LIMIT %s
    """
    
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return [DiagnosisData(**row) for row in results]

@app.get("/analytics/providers", response_model=List[ProviderData])
async def get_providers(current_user: User = Depends(get_current_user)):
    """Get provider utilization"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        pr.specialty,
        COUNT(*) as visits,
        COUNT(DISTINCT pr.provider_id) as providers,
        ROUND(AVG(f.cost), 2) as avg_cost
    FROM public.fact_visits f
    JOIN public.dim_providers pr ON f.provider_key = pr.provider_key
    GROUP BY pr.specialty
    ORDER BY visits DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return [ProviderData(**row) for row in results]

@app.get("/patients/{patient_id}/visits", response_model=List[Visit])
async def get_patient_visits(patient_id: int, current_user: User = Depends(get_current_user)):
    """Get anonymized visit history for a patient"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        f.visit_id::int,
        f.visit_date::text,
        f.visit_type,
        f.diagnosis,
        f.cost,
        pr.specialty as provider_specialty
    FROM public.fact_visits f
    JOIN public.dim_patients p ON f.patient_key = p.patient_key
    JOIN public.dim_providers pr ON f.provider_key = pr.provider_key
    WHERE p.patient_id = %s
    ORDER BY f.visit_date DESC
    """
    
    cursor.execute(query, (patient_id,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    if not results:
        raise HTTPException(status_code=404, detail="Patient not found or has no visits")
    
    return [Visit(**row) for row in results]

@app.get("/predictions/{patient_id}", response_model=List[Prediction])
async def get_predictions(patient_id: int, current_user: User = Depends(get_current_user)):
    """Get ML predictions for a patient"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if predictions table exists
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'ml_predictions'
        )
    """)
    
    table_exists = cursor.fetchone()['exists']
    
    if not table_exists:
        cursor.close()
        conn.close()
        raise HTTPException(
            status_code=503,
            detail="ML predictions not available. Please run ml_pipeline.py first."
        )
    
    query = """
    SELECT 
        patient_id,
        visit_date::text,
        readmission_risk,
        predicted_cost,
        actual_cost,
        CASE WHEN is_anomaly = 1 THEN true ELSE false END as is_anomaly
    FROM ml_predictions
    WHERE patient_id = %s
    ORDER BY visit_date DESC
    """
    
    cursor.execute(query, (patient_id,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    if not results:
        raise HTTPException(status_code=404, detail="No predictions found for this patient")
    
    return [Prediction(**row) for row in results]

@app.get("/predictions/anomalies", response_model=List[Prediction])
async def get_anomalies(limit: int = 50, current_user: User = Depends(get_current_user)):
    """Get detected anomalies"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        patient_id,
        visit_date::text,
        readmission_risk,
        predicted_cost,
        actual_cost,
        CASE WHEN is_anomaly = 1 THEN true ELSE false END as is_anomaly
    FROM ml_predictions
    WHERE is_anomaly = 1
    ORDER BY anomaly_score
    LIMIT %s
    """
    
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return [Prediction(**row) for row in results]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
