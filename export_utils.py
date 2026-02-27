"""
Healthcare Data Warehouse - Export Utilities
Provides export functionality for CSV, Excel, and PDF reports
"""

import pandas as pd
import psycopg2
from datetime import datetime
import os


def get_connection():
    """Get database connection"""
    return psycopg2.connect(
        host="localhost",
        port=5433,
        database="health_dw",
        user="user",
        password="pass"
    )


def export_to_csv(query, filename):
    """
    Export query results to CSV
    
    Args:
        query: SQL query string
        filename: Output CSV filename
    """
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    
    output_path = f"exports/{filename}"
    os.makedirs("exports", exist_ok=True)
    df.to_csv(output_path, index=False)
    
    print(f"✅ Exported {len(df)} rows to {output_path}")
    return output_path


def export_to_excel(queries_dict, filename):
    """
    Export multiple queries to Excel with separate sheets
    
    Args:
        queries_dict: Dictionary of {sheet_name: query}
        filename: Output Excel filename
    """
    conn = get_connection()
    
    output_path = f"exports/{filename}"
    os.makedirs("exports", exist_ok=True)
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for sheet_name, query in queries_dict.items():
            df = pd.read_sql(query, conn)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"✅ Added sheet '{sheet_name}' with {len(df)} rows")
    
    conn.close()
    print(f"✅ Exported to {output_path}")
    return output_path


def generate_analytics_report():
    """
    Generate comprehensive analytics report with all key metrics
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    queries = {
        "KPIs": """
            SELECT 
                (SELECT COUNT(DISTINCT patient_id) FROM public.dim_patients) as total_patients,
                (SELECT COUNT(DISTINCT provider_id) FROM public.dim_providers) as total_providers,
                (SELECT COUNT(*) FROM public.fact_visits) as total_visits,
                (SELECT ROUND(AVG(cost), 2) FROM public.fact_visits) as avg_cost
        """,
        "Age_Groups": """
            SELECT 
                p.age_group,
                COUNT(*) as visit_count,
                COUNT(DISTINCT p.patient_id) as unique_patients,
                ROUND(AVG(f.cost), 2) as avg_cost
            FROM public.fact_visits f
            JOIN public.dim_patients p ON f.patient_key = p.patient_key
            GROUP BY p.age_group
            ORDER BY visit_count DESC
        """,
        "Top_Diagnoses": """
            SELECT 
                diagnosis,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM public.fact_visits), 2) as percentage
            FROM public.fact_visits
            GROUP BY diagnosis
            ORDER BY count DESC
            LIMIT 20
        """,
        "Provider_Stats": """
            SELECT 
                pr.specialty,
                COUNT(*) as visits,
                COUNT(DISTINCT pr.provider_id) as providers,
                ROUND(AVG(f.cost), 2) as avg_cost
            FROM public.fact_visits f
            JOIN public.dim_providers pr ON f.provider_key = pr.provider_key
            GROUP BY pr.specialty
            ORDER BY visits DESC
        """,
        "Monthly_Trends": """
            SELECT 
                DATE_TRUNC('month', visit_date)::date as month,
                COUNT(*) as visits,
                ROUND(AVG(cost), 2) as avg_cost
            FROM public.fact_visits
            GROUP BY month
            ORDER BY month
        """
    }
    
    filename = f"healthcare_analytics_report_{timestamp}.xlsx"
    return export_to_excel(queries, filename)


def export_patient_data(patient_id):
    """
    Export all data for a specific patient (anonymized)
    """
    query = f"""
    SELECT 
        f.visit_date,
        f.visit_type,
        f.diagnosis,
        f.procedure_performed,
        f.cost,
        pr.specialty as provider_specialty
    FROM public.fact_visits f
    JOIN public.dim_patients p ON f.patient_key = p.patient_key
    JOIN public.dim_providers pr ON f.provider_key = pr.provider_key
    WHERE p.patient_id = {patient_id}
    ORDER BY f.visit_date DESC
    """
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"patient_{patient_id}_data_{timestamp}.csv"
    return export_to_csv(query, filename)


def export_ml_predictions(limit=1000):
    """
    Export ML predictions
    """
    query = f"""
    SELECT 
        patient_id,
        visit_date,
        readmission_risk,
        predicted_cost,
        actual_cost,
        is_anomaly,
        anomaly_score
    FROM ml_predictions
    ORDER BY prediction_date DESC
    LIMIT {limit}
    """
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"ml_predictions_{timestamp}.csv"
    return export_to_csv(query, filename)


if __name__ == "__main__":
    print("=" * 60)
    print("Healthcare Data Warehouse - Export Utilities")
    print("=" * 60)
    
    # Generate comprehensive report
    print("\n1. Generating comprehensive analytics report...")
    report_file = generate_analytics_report()
    
    # Export sample patient data
    print("\n2. Exporting sample patient data...")
    patient_file = export_patient_data(patient_id=1)
    
    print("\n3. Exporting ML predictions...")
    try:
        predictions_file = export_ml_predictions(limit=1000)
    except Exception as e:
        print(f"⚠️  ML predictions not available yet: {e}")
    
    print("\n" + "=" * 60)
    print("✅ Export complete! Check the 'exports/' directory")
    print("=" * 60)
