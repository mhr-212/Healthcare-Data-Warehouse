"""
Privacy Metrics Dashboard
Interactive visualization of privacy guarantees and compliance
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from privacy_engine import PrivacyEngine
import psycopg2
import json
from datetime import datetime

st.set_page_config(
    page_title="Privacy Metrics Dashboard",
    page_icon="🔐",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
    }
    .pass { color: #00ff00; font-weight: bold; }
    .fail { color: #ff4444; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# Database connection
def get_connection():
    """Get fresh database connection"""
    return psycopg2.connect(
        host="localhost",
        port=5433,
        database="health_dw",
        user="user",
        password="pass"
    )

# Load data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data(limit=10000):
    conn = get_connection()
    query = f"""
    SELECT 
        p.age_group,
        p.gender,
        p.state,
        f.diagnosis,
        f.visit_type,
        f.cost
    FROM public.fact_visits f
    JOIN public.dim_patients p ON f.patient_key = p.patient_key
    LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Run privacy audit
@st.cache_data
def run_privacy_audit(k_val, l_val, t_val):
    df = load_data()
    engine = PrivacyEngine(k=k_val, l=l_val, t=t_val)
    
    quasi_identifiers = ['age_group', 'gender', 'state']
    sensitive_attributes = ['diagnosis', 'visit_type']
    
    return engine.comprehensive_privacy_audit(df, quasi_identifiers, sensitive_attributes)

# Main UI
st.title("🔐 Privacy Metrics Dashboard")
st.markdown("**Real-time privacy compliance monitoring for healthcare data**")

st.markdown("---")

# Sidebar configuration
with st.sidebar:
    st.header("Privacy Parameters")
    
    k_value = st.slider("K-anonymity (k)", min_value=2, max_value=20, value=5,
                       help="Minimum group size for anonymity")
    l_value = st.slider("L-diversity (l)", min_value=2, max_value=10, value=3,
                       help="Minimum diversity in sensitive attributes")
    t_value = st.slider("T-closeness (t)", min_value=0.1, max_value=0.5, value=0.2, step=0.05,
                       help="Maximum distribution distance")
    
    if st.button("🔄 Run Privacy Audit", type="primary"):
        st.cache_data.clear()
        st.rerun()

# Run audit
audit = run_privacy_audit(k_value, l_value, t_value)

# Overall Score
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    score = audit['overall_privacy_score']
    color = "green" if score >= 80 else "yellow" if score >= 60 else "red"
    
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        title={"text": "Overall Privacy Score"},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {'color': color},
            'steps': [
                {'range': [0, 60], 'color': "lightgray"},
                {'range': [60, 80], 'color': "lightyellow"},
                {'range': [80, 100], 'color': "lightgreen"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 90
            }
        }
    ))
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.metric("Records Analyzed", f"{audit['record_count']:,}")
    st.metric("Audit Time", datetime.now().strftime("%H:%M"))

with col3:
    k_pass = audit['k_anonymity']['satisfies_k_anonymity']
    st.metric("K-anonymity", "✅ PASS" if k_pass else "❌ FAIL")
    st.metric("Timestamp", datetime.now().strftime("%Y-%m-%d"))

st.markdown("---")

# K-anonymity Details
st.subheader(f"📊 K-Anonymity Analysis (k={k_value})")

col1, col2, col3, col4 = st.columns(4)
k_results = audit['k_anonymity']

with col1:
    st.metric("Total Groups", k_results['total_groups'])
with col2:
    st.metric("Violating Groups", k_results['violating_groups'],
              delta=f"-{k_results['violating_groups']}" if k_results['violating_groups'] > 0 else "0",
              delta_color="inverse")
with col3:
    st.metric("Smallest Group", k_results['smallest_group_size'])
with col4:
    st.metric("Avg Group Size", f"{k_results['average_group_size']:.1f}")

if k_results['records_at_risk'] > 0:
    st.warning(f"⚠️ {k_results['records_at_risk']} records at risk due to small group sizes")

# L-diversity Results
st.markdown("---")
st.subheader(f"🎭 L-Diversity Analysis (l={l_value})")

l_div_data = []
for attr, results in audit['l_diversity'].items():
    l_div_data.append({
        "Attribute": attr.title(),
        "Pass/Fail": "✅ PASS" if results['satisfies_l_diversity'] else "❌ FAIL",
        "Total Groups": results['total_groups'],
        "Violating Groups": results['violating_groups'],
        "Min Diversity": results['min_diversity'],
        "Avg Diversity": f"{results['avg_diversity']:.1f}"
    })

st.dataframe(pd.DataFrame(l_div_data), use_container_width=True, hide_index=True)

# T-closeness Results
st.markdown("---")
st.subheader(f"📏 T-Closeness Analysis (t={t_value})")

t_close_data = []
for attr, results in audit['t_closeness'].items():
    t_close_data.append({
        "Attribute": attr.title(),
        "Pass/Fail": "✅ PASS" if results['satisfies_t_closeness'] else "❌ FAIL",
        "Total Groups": results['total_groups'],
        "Violating Groups": results['violating_groups'],
        "Max Distance": f"{results['max_distance']:.4f}",
        "Avg Distance": f"{results['avg_distance']:.4f}"
    })

st.dataframe(pd.DataFrame(t_close_data), use_container_width=True, hide_index=True)

# Recommendations
st.markdown("---")
st.subheader("💡 Recommendations")

if audit['overall_privacy_score'] < 100:
    st.info("**Privacy improvements needed:**")
    
    if not k_results['satisfies_k_anonymity']:
        st.warning(f"- Increase group sizes or suppress records in groups < {k_value}")
    
    for attr, results in audit['l_diversity'].items():
        if not results['satisfies_l_diversity']:
            st.warning(f"- Increase diversity of '{attr}' in small groups (current min: {results['min_diversity']})")
    
    for attr, results in audit['t_closeness'].items():
        if not results['satisfies_t_closeness']:
            st.warning(f"- Reduce distribution skew for '{attr}' (max distance: {results['max_distance']:.4f})")
else:
    st.success("✅ **All privacy requirements satisfied!** Your data meets k-anonymity, l-diversity, and t-closeness standards.")

# Export Report
st.markdown("---")
col1, col2 = st.columns(2)

with col1:
    if st.button("📥 Download Privacy Report (JSON)"):
        report_json = json.dumps(audit, indent=2, default=str)
        st.download_button(
            label="Download JSON",
            data=report_json,
            file_name=f"privacy_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )

with col2:
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Footer
st.markdown("---")
st.caption("🔐 Advanced Privacy Metrics Dashboard | K-anonymity • L-diversity • T-closeness")
