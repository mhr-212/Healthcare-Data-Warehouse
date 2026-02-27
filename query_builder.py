"""
Healthcare Data Warehouse - Simple Query Builder Web UI
Allows non-technical users to run pre-defined queries without SQL knowledge
"""

import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="Query Builder - Healthcare DW",
    page_icon="🔍",
    layout="wide"
)

# Database connection
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5433,
        database="health_dw",
        user="user",
        password="pass"
    )

# Pre-defined queries
QUERIES = {
    "Patient Summary": {
        "description": "Get summary statistics for all patients",
        "query": """
            SELECT 
                age_group,
                gender,
                COUNT(*) as patient_count
            FROM public.dim_patients
            GROUP BY age_group, gender
            ORDER BY age_group, gender
        """,
        "params": []
    },
    "Visits by Age Group": {
        "description": "Count visits grouped by patient age",
        "query": """
            SELECT 
                p.age_group,
                COUNT(*) as visit_count,
                ROUND(AVG(f.cost), 2) as avg_cost
            FROM public.fact_visits f
            JOIN public.dim_patients p ON f.patient_key = p.patient_key
            GROUP BY p.age_group
            ORDER BY visit_count DESC
        """,
        "params": []
    },
    "Top Diagnoses": {
        "description": "Most common diagnoses",
        "query": """
            SELECT 
                diagnosis,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM public.fact_visits), 2) as percentage
            FROM public.fact_visits
            GROUP BY diagnosis
            ORDER BY count DESC
            LIMIT {limit}
        """,
        "params": ["limit"]
    },
    "Patient Visit History": {
        "description": "Get visit history for a specific patient",
        "query": """
            SELECT 
                f.visit_date,
                f.visit_type,
                f.diagnosis,
                f.cost,
                pr.specialty
            FROM public.fact_visits f
            JOIN public.dim_patients p ON f.patient_key = p.patient_key
            JOIN public.dim_providers pr ON f.provider_key = pr.provider_key
            WHERE p.patient_id = {patient_id}
            ORDER BY f.visit_date DESC
        """,
        "params": ["patient_id"]
    },
    "High Cost Visits": {
        "description": "Visits with costs above threshold",
        "query": """
            SELECT 
                p.patient_id,
                p.age_group,
                f.visit_date,
                f.diagnosis,
                f.cost
            FROM public.fact_visits f
            JOIN public.dim_patients p ON f.patient_key = p.patient_key
            WHERE f.cost > {cost_threshold}
            ORDER BY f.cost DESC
            LIMIT 100
        """,
        "params": ["cost_threshold"]
    },
    "Provider Workload": {
        "description": "Provider statistics by specialty",
        "query": """
            SELECT 
                pr.specialty,
                COUNT(DISTINCT pr.provider_id) as providers,
                COUNT(*) as total_visits,
                ROUND(AVG(f.cost), 2) as avg_cost
            FROM public.fact_visits f
            JOIN public.dim_providers pr ON f.provider_key = pr.provider_key
            GROUP BY pr.specialty
            ORDER BY total_visits DESC
        """,
        "params": []
    },
    "Anomalies Detected": {
        "description": "ML-detected anomalies (requires ML pipeline)",
        "query": """
            SELECT 
                patient_id,
                visit_date,
                predicted_cost,
                actual_cost,
                anomaly_score
            FROM ml_predictions
            WHERE is_anomaly = 1
            ORDER BY anomaly_score
            LIMIT {limit}
        """,
        "params": ["limit"]
    }
}

# Main UI
st.title("🔍 Healthcare Query Builder")
st.markdown("Run pre-defined queries without writing SQL")

st.markdown("---")

# Query selection
query_name = st.selectbox(
    "Select a Query",
    options=list(QUERIES.keys())
)

query_info = QUERIES[query_name]
st.info(f"**Description**: {query_info['description']}")

# Parameter inputs
params = {}
if query_info['params']:
    st.subheader("Query Parameters")
    cols = st.columns(len(query_info['params']))
    
    for idx, param in enumerate(query_info['params']):
        with cols[idx]:
            if param == "limit":
                params[param] = st.number_input("Limit Results", min_value=1, max_value=1000, value=10)
            elif param == "patient_id":
                params[param] = st.number_input("Patient ID", min_value=1, max_value=10000, value=1)
            elif param == "cost_threshold":
                params[param] = st.number_input("Cost Threshold ($)", min_value=0.0, value=5000.0, step=100.0)

# Run query button
if st.button("🚀 Run Query", type="primary"):
    try:
        conn = get_connection()
        
        # Format query with parameters
        query = query_info['query'].format(**params) if params else query_info['query']
        
        # Execute query
        with st.spinner("Executing query..."):
            df = pd.read_sql(query, conn)
        
        conn.close()
        
        # Display results
        st.success(f"✅ Query executed successfully! Found {len(df)} rows.")
        
        # Show results
        st.subheader("Query Results")
        st.dataframe(df, use_container_width=True)
        
        # Download button
        csv = df.to_csv(index=False).encode('utf-8')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{query_name.replace(' ', '_')}_{timestamp}.csv"
        
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=filename,
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"❌ Error executing query: {e}")

# SQL Preview (expandable)
with st.expander("📝 View SQL Query"):
    query_preview = query_info['query'].format(**params) if params else query_info['query']
    st.code(query_preview, language="sql")

# Footer
st.markdown("---")
st.caption("Healthcare Data Warehouse Query Builder | No SQL knowledge required!")
