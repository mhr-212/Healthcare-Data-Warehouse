"""
Healthcare Data Warehouse - Interactive Analytics Dashboard
Built with Streamlit, Plotly, and PostgreSQL

Features:
- Real-time KPI metrics
- Interactive visualizations
- Age group and diagnosis analysis
- Provider utilization tracking
- Geographic distribution
- Differential privacy comparison
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Healthcare Analytics Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
    }
</style>
""", unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_connection():
    """Create and cache database connection"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5433,
            database="health_dw",
            user="user",
            password="pass"
        )
        return conn
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# Data loading functions with caching
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_kpi_metrics(_conn):
    """Load key performance indicators"""
    query = """
    SELECT 
        (SELECT COUNT(DISTINCT patient_id) FROM public.dim_patients) as total_patients,
        (SELECT COUNT(DISTINCT provider_id) FROM public.dim_providers) as total_providers,
        (SELECT COUNT(*) FROM public.fact_visits) as total_visits,
        (SELECT ROUND(AVG(cost), 2) FROM public.fact_visits) as avg_cost,
        (SELECT ROUND(AVG(cost_with_privacy), 2) FROM public.fact_visits) as avg_cost_privacy
    """
    return pd.read_sql(query, _conn)

@st.cache_data(ttl=300)
def load_age_group_data(_conn):
    """Load age group analysis"""
    query = """
    SELECT 
        p.age_group,
        COUNT(*) as visit_count,
        COUNT(DISTINCT p.patient_id) as unique_patients,
        ROUND(AVG(f.cost), 2) as avg_cost,
        ROUND(AVG(f.cost_with_privacy), 2) as avg_cost_privacy
    FROM public.fact_visits f
    JOIN public.dim_patients p ON f.patient_key = p.patient_key
    GROUP BY p.age_group
    ORDER BY visit_count DESC
    """
    return pd.read_sql(query, _conn)

@st.cache_data(ttl=300)
def load_diagnosis_data(_conn):
    """Load diagnosis distribution"""
    query = """
    SELECT 
        diagnosis,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM public.fact_visits), 2) as percentage
    FROM public.fact_visits
    GROUP BY diagnosis
    ORDER BY count DESC
    LIMIT 15
    """
    return pd.read_sql(query, _conn)

@st.cache_data(ttl=300)
def load_provider_data(_conn):
    """Load provider utilization"""
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
    return pd.read_sql(query, _conn)

@st.cache_data(ttl=300)
def load_time_series_data(_conn):
    """Load time series data for trends"""
    query = """
    SELECT 
        DATE_TRUNC('month', visit_date) as month,
        COUNT(*) as visits,
        ROUND(AVG(cost), 2) as avg_cost
    FROM public.fact_visits
    GROUP BY month
    ORDER BY month
    """
    return pd.read_sql(query, _conn)

@st.cache_data(ttl=300)
def load_geographic_data(_conn):
    """Load geographic distribution"""
    query = """
    SELECT 
        p.state,
        COUNT(DISTINCT p.patient_id) as patient_count,
        COUNT(*) as visit_count
    FROM public.fact_visits f
    JOIN public.dim_patients p ON f.patient_key = p.patient_key
    GROUP BY p.state
    ORDER BY patient_count DESC
    LIMIT 20
    """
    return pd.read_sql(query, _conn)

@st.cache_data(ttl=300)
def load_visit_type_data(_conn):
    """Load visit type distribution"""
    query = """
    SELECT 
        visit_type,
        COUNT(*) as count
    FROM public.fact_visits
    GROUP BY visit_type
    ORDER BY count DESC
    """
    return pd.read_sql(query, _conn)

# Main dashboard
def main():
    # Header
    st.markdown('<h1 class="main-header">🏥 Healthcare Analytics Dashboard</h1>', unsafe_allow_html=True)
    
    # Get database connection
    conn = get_connection()
    if not conn:
        st.stop()
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    st.sidebar.markdown("---")
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.info("**Data Source**: PostgreSQL Healthcare DW")
    st.sidebar.metric("Last Updated", datetime.now().strftime("%Y-%m-%d %H:%M"))
    
    # Load data
    try:
        kpi_data = load_kpi_metrics(conn)
        age_data = load_age_group_data(conn)
        diagnosis_data = load_diagnosis_data(conn)
        provider_data = load_provider_data(conn)
        time_data = load_time_series_data(conn)
        geo_data = load_geographic_data(conn)
        visit_type_data = load_visit_type_data(conn)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.stop()
    
    # KPI Cards
    st.subheader("📊 Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="👥 Total Patients",
            value=f"{kpi_data['total_patients'].iloc[0]:,}",
            delta="Anonymized"
        )
    
    with col2:
        st.metric(
            label="🏥 Total Visits",
            value=f"{kpi_data['total_visits'].iloc[0]:,}",
            delta=f"{kpi_data['total_visits'].iloc[0] / kpi_data['total_patients'].iloc[0]:.2f} avg/patient"
        )
    
    with col3:
        st.metric(
            label="👨‍⚕️ Active Providers",
            value=f"{kpi_data['total_providers'].iloc[0]:,}",
            delta="13 Specialties"
        )
    
    with col4:
        st.metric(
            label="💰 Avg Visit Cost",
            value=f"${kpi_data['avg_cost'].iloc[0]:,.2f}",
            delta=f"±${abs(kpi_data['avg_cost_privacy'].iloc[0] - kpi_data['avg_cost'].iloc[0]):.2f} privacy"
        )
    
    st.markdown("---")
    
    # Tabs for different analyses
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Age Group Analysis",
        "🔬 Diagnosis Insights",
        "👨‍⚕️ Provider Utilization",
        "📅 Time Trends",
        "🗺️ Geographic Distribution"
    ])
    
    # Tab 1: Age Group Analysis
    with tab1:
        st.subheader("Age Group Distribution & Metrics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Age group visits bar chart
            fig_age_visits = px.bar(
                age_data,
                x='age_group',
                y='visit_count',
                title='Visits by Age Group',
                labels={'visit_count': 'Number of Visits', 'age_group': 'Age Group'},
                color='visit_count',
                color_continuous_scale='Blues'
            )
            fig_age_visits.update_layout(showlegend=False)
            st.plotly_chart(fig_age_visits, use_container_width=True)
        
        with col2:
            # Patient distribution pie chart
            fig_age_patients = px.pie(
                age_data,
                values='unique_patients',
                names='age_group',
                title='Patient Distribution by Age Group',
                hole=0.4
            )
            st.plotly_chart(fig_age_patients, use_container_width=True)
        
        # Age group metrics table
        st.subheader("Detailed Age Group Metrics")
        age_data_display = age_data.copy()
        age_data_display['avg_visits_per_patient'] = (
            age_data_display['visit_count'] / age_data_display['unique_patients']
        ).round(2)
        st.dataframe(
            age_data_display,
            use_container_width=True,
            hide_index=True
        )
    
    # Tab 2: Diagnosis Insights
    with tab2:
        st.subheader("Top Diagnoses & Distribution")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Diagnosis bar chart
            fig_diagnosis = px.bar(
                diagnosis_data,
                x='count',
                y='diagnosis',
                orientation='h',
                title='Top 15 Diagnoses',
                labels={'count': 'Number of Cases', 'diagnosis': 'Diagnosis'},
                color='percentage',
                color_continuous_scale='Viridis',
                text='percentage'
            )
            fig_diagnosis.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
            fig_diagnosis.update_layout(height=600)
            st.plotly_chart(fig_diagnosis, use_container_width=True)
        
        with col2:
            st.metric("Total Unique Diagnoses", len(diagnosis_data))
            st.metric("Most Common", diagnosis_data.iloc[0]['diagnosis'])
            st.metric("Prevalence", f"{diagnosis_data.iloc[0]['percentage']}%")
            
            # Show top 5 in table
            st.subheader("Top 5 Summary")
            st.dataframe(
                diagnosis_data.head(5)[['diagnosis', 'count', 'percentage']],
                hide_index=True,
                use_container_width=True
            )
    
    # Tab 3: Provider Utilization
    with tab3:
        st.subheader("Healthcare Provider Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Specialty utilization
            fig_specialty = px.bar(
                provider_data,
                x='specialty',
                y='visits',
                title='Visits by Medical Specialty',
                labels={'visits': 'Number of Visits', 'specialty': 'Specialty'},
                color='avg_cost',
                color_continuous_scale='RdYlGn_r'
            )
            fig_specialty.update_xaxes(tickangle=-45)
            st.plotly_chart(fig_specialty, use_container_width=True)
        
        with col2:
            # Provider density
            fig_providers = px.scatter(
                provider_data,
                x='providers',
                y='visits',
                size='avg_cost',
                color='specialty',
                title='Provider Workload Analysis',
                labels={
                    'providers': 'Number of Providers',
                    'visits': 'Total Visits',
                    'avg_cost': 'Avg Cost'
                },
                hover_data=['specialty', 'avg_cost']
            )
            st.plotly_chart(fig_providers, use_container_width=True)
        
        # Provider metrics table
        st.subheader("Specialty Metrics")
        provider_display = provider_data.copy()
        provider_display['visits_per_provider'] = (
            provider_display['visits'] / provider_display['providers']
        ).round(2)
        st.dataframe(provider_display, hide_index=True, use_container_width=True)
    
    # Tab 4: Time Trends
    with tab4:
        st.subheader("Temporal Trends & Patterns")
        
        # Monthly visits trend
        fig_time = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Monthly Visit Volume', 'Average Monthly Cost'),
            vertical_spacing=0.15
        )
        
        fig_time.add_trace(
            go.Scatter(
                x=time_data['month'],
                y=time_data['visits'],
                mode='lines+markers',
                name='Visits',
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=8)
            ),
            row=1, col=1
        )
        
        fig_time.add_trace(
            go.Scatter(
                x=time_data['month'],
                y=time_data['avg_cost'],
                mode='lines+markers',
                name='Avg Cost',
                line=dict(color='#ff7f0e', width=3),
                marker=dict(size=8)
            ),
            row=2, col=1
        )
        
        fig_time.update_xaxes(title_text="Month", row=2, col=1)
        fig_time.update_yaxes(title_text="Number of Visits", row=1, col=1)
        fig_time.update_yaxes(title_text="Average Cost ($)", row=2, col=1)
        fig_time.update_layout(height=700, showlegend=False)
        
        st.plotly_chart(fig_time, use_container_width=True)
        
        # Visit type distribution
        col1, col2 = st.columns(2)
        with col1:
            fig_visit_type = px.pie(
                visit_type_data,
                values='count',
                names='visit_type',
                title='Visit Type Distribution'
            )
            st.plotly_chart(fig_visit_type, use_container_width=True)
        
        with col2:
            st.subheader("Time Period Summary")
            st.metric("Total Months", len(time_data))
            st.metric("Peak Month Visits", time_data['visits'].max())
            st.metric("Avg Monthly Visits", f"{time_data['visits'].mean():.0f}")
    
    # Tab 5: Geographic Distribution
    with tab5:
        st.subheader("Patient Geographic Distribution")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # State distribution
            fig_geo = px.bar(
                geo_data,
                x='state',
                y='patient_count',
                title='Top 20 States by Patient Count',
                labels={'patient_count': 'Number of Patients', 'state': 'State'},
                color='visit_count',
                color_continuous_scale='Teal'
            )
            fig_geo.update_layout(height=500)
            st.plotly_chart(fig_geo, use_container_width=True)
        
        with col2:
            st.subheader("Geographic Summary")
            st.metric("States Represented", len(geo_data))
            st.metric("Top State", geo_data.iloc[0]['state'])
            st.metric("Patients in Top State", f"{geo_data.iloc[0]['patient_count']:,}")
            
            # Visits per patient by state
            geo_display = geo_data.copy()
            geo_display['visits_per_patient'] = (
                geo_display['visit_count'] / geo_display['patient_count']
            ).round(2)
            
            st.subheader("Top 10 States")
            st.dataframe(
                geo_display.head(10)[['state', 'patient_count', 'visits_per_patient']],
                hide_index=True,
                use_container_width=True
            )
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p><strong>Healthcare Data Warehouse Analytics Dashboard</strong></p>
        <p>📊 Data Privacy: All patient data is anonymized | 🔐 HIPAA Compliant | 🎯 Differential Privacy Enabled</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
