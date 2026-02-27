"""
Healthcare Data Warehouse - Advanced Privacy Engine
Implements K-anonymity, L-diversity, T-closeness, and Privacy Budget Tracking

Privacy Guarantees:
- K-anonymity: Each record is indistinguishable from at least k-1 other records
- L-diversity: Each equivalence class has at least L well-represented sensitive values
- T-closeness: Distribution of sensitive attributes close to overall distribution
- Differential Privacy: Already implemented in dbt models
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
import psycopg2
from collections import Counter
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PrivacyEngine:
    """Advanced Privacy Techniques for Healthcare Data"""
    
    def __init__(self, k=5, l=3, t=0.2):
        """
        Initialize privacy engine
        
        Args:
            k: Minimum group size for k-anonymity
            l: Minimum diversity for l-diversity
            t: Maximum distribution distance for t-closeness
        """
        self.k = k
        self.l = l
        self.t = t
        self.privacy_budget = {"epsilon": 0, "queries": []}
        
    def check_k_anonymity(self, df: pd.DataFrame, quasi_identifiers: List[str]) -> Dict:
        """
        Check if dataset satisfies k-anonymity
        
        Args:
            df: DataFrame to check
            quasi_identifiers: Columns that could be used to identify individuals
            
        Returns:
            Dictionary with k-anonymity metrics
        """
        logger.info(f"Checking {self.k}-anonymity on {len(df)} records...")
        
        # Group by quasi-identifiers
        grouped = df.groupby(quasi_identifiers).size().reset_index(name='count')
        
        # Find groups smaller than k
        violations = grouped[grouped['count'] < self.k]
        
        results = {
            "satisfies_k_anonymity": len(violations) == 0,
            "k_value": self.k,
            "total_groups": len(grouped),
            "violating_groups": len(violations),
            "smallest_group_size": grouped['count'].min(),
            "largest_group_size": grouped['count'].max(),
            "average_group_size": grouped['count'].mean(),
            "records_at_risk": violations['count'].sum() if len(violations) > 0 else 0
        }
        
        logger.info(f"K-anonymity check: {results['satisfies_k_anonymity']}")
        logger.info(f"Smallest group: {results['smallest_group_size']}, " 
                   f"Violating groups: {results['violating_groups']}")
        
        return results
    
    def enforce_k_anonymity(self, df: pd.DataFrame, quasi_identifiers: List[str],
                           method='suppress') -> pd.DataFrame:
        """
        Enforce k-anonymity by suppressing or generalizing records
        
        Args:
            df: Input DataFrame
            quasi_identifiers: Quasi-identifier columns
            method: 'suppress' (remove small groups) or 'generalize' (merge groups)
            
        Returns:
            K-anonymous DataFrame
        """
        logger.info(f"Enforcing {self.k}-anonymity using {method} method...")
        
        # Count group sizes
        df['_group_size'] = df.groupby(quasi_identifiers)[quasi_identifiers[0]].transform('count')
        
        if method == 'suppress':
            # Remove records in groups smaller than k
            df_anonymous = df[df['_group_size'] >= self.k].copy()
            suppressed = len(df) - len(df_anonymous)
            logger.info(f"Suppressed {suppressed} records ({suppressed/len(df)*100:.2f}%)")
            
        elif method == 'generalize':
            # For age_group, generalize to broader categories
            if 'age_group' in quasi_identifiers:
                df_anonymous = df.copy()
                # Merge adjacent age groups for small groups
                small_groups = df[df['_group_size'] < self.k]
                for idx in small_groups.index:
                    age = df.loc[idx, 'age_group']
                    # Generalize to "Adult" or "Senior"
                    if age in ['18-30', '31-45', '46-60']:
                        df_anonymous.loc[idx, 'age_group'] = 'Adult (18-60)'
                    else:
                        df_anonymous.loc[idx, 'age_group'] = 'Senior (60+)'
                        
                logger.info(f"Generalized {len(small_groups)} records")
            else:
                df_anonymous = df[df['_group_size'] >= self.k].copy()
        
        # Remove helper column
        df_anonymous = df_anonymous.drop('_group_size', axis=1)
        
        return df_anonymous
    
    def check_l_diversity(self, df: pd.DataFrame, quasi_identifiers: List[str],
                         sensitive_attribute: str) -> Dict:
        """
        Check if dataset satisfies l-diversity
        
        L-diversity ensures each equivalence class has at least L well-represented
        values for sensitive attributes
        
        Args:
            df: DataFrame to check
            quasi_identifiers: Quasi-identifier columns
            sensitive_attribute: Sensitive column (e.g., diagnosis)
            
        Returns:
            Dictionary with l-diversity metrics
        """
        logger.info(f"Checking {self.l}-diversity for '{sensitive_attribute}'...")
        
        # Group by quasi-identifiers and count unique sensitive values
        diversity_check = df.groupby(quasi_identifiers)[sensitive_attribute].agg([
            ('unique_count', 'nunique'),
            ('total_count', 'size')
        ]).reset_index()
        
        # Check violations
        violations = diversity_check[diversity_check['unique_count'] < self.l]
        
        results = {
            "satisfies_l_diversity": len(violations) == 0,
            "l_value": self.l,
            "sensitive_attribute": sensitive_attribute,
            "total_groups": len(diversity_check),
            "violating_groups": len(violations),
            "min_diversity": diversity_check['unique_count'].min(),
            "max_diversity": diversity_check['unique_count'].max(),
            "avg_diversity": diversity_check['unique_count'].mean()
        }
        
        logger.info(f"L-diversity check: {results['satisfies_l_diversity']}")
        logger.info(f"Min diversity: {results['min_diversity']}, "
                   f"Violating groups: {results['violating_groups']}")
        
        return results
    
    def calculate_earth_movers_distance(self, dist1: Counter, dist2: Counter) -> float:
        """
        Calculate Earth Mover's Distance between two distributions
        Simplified version for categorical data
        """
        all_keys = set(dist1.keys()) | set(dist2.keys())
        total1 = sum(dist1.values())
        total2 = sum(dist2.values())
        
        distance = 0
        for key in all_keys:
            p1 = dist1.get(key, 0) / total1
            p2 = dist2.get(key, 0) / total2
            distance += abs(p1 - p2)
        
        return distance / 2  # Normalize
    
    def check_t_closeness(self, df: pd.DataFrame, quasi_identifiers: List[str],
                         sensitive_attribute: str) -> Dict:
        """
        Check if dataset satisfies t-closeness
        
        T-closeness ensures distribution of sensitive attribute in each group
        is close to the overall distribution
        
        Args:
            df: DataFrame to check
            quasi_identifiers: Quasi-identifier columns
            sensitive_attribute: Sensitive column
            
        Returns:
            Dictionary with t-closeness metrics
        """
        logger.info(f"Checking {self.t}-closeness for '{sensitive_attribute}'...")
        
        # Overall distribution
        overall_dist = Counter(df[sensitive_attribute])
        
        # Check each equivalence class
        violations = []
        distances = []
        
        for group_values, group_df in df.groupby(quasi_identifiers):
            group_dist = Counter(group_df[sensitive_attribute])
            distance = self.calculate_earth_movers_distance(group_dist, overall_dist)
            distances.append(distance)
            
            if distance > self.t:
                violations.append({
                    'group': group_values,
                    'distance': distance,
                    'size': len(group_df)
                })
        
        results = {
            "satisfies_t_closeness": len(violations) == 0,
            "t_value": self.t,
            "sensitive_attribute": sensitive_attribute,
            "total_groups": len(distances),
            "violating_groups": len(violations),
            "max_distance": max(distances) if distances else 0,
            "avg_distance": np.mean(distances) if distances else 0,
            "violations": violations[:10]  # Top 10 violations
        }
        
        logger.info(f"T-closeness check: {results['satisfies_t_closeness']}")
        logger.info(f"Max distance: {results['max_distance']:.4f}, "
                   f"Violating groups: {results['violating_groups']}")
        
        return results
    
    def comprehensive_privacy_audit(self, df: pd.DataFrame,
                                   quasi_identifiers: List[str],
                                   sensitive_attributes: List[str]) -> Dict:
        """
        Run comprehensive privacy audit checking all privacy metrics
        
        Args:
            df: DataFrame to audit
            quasi_identifiers: Quasi-identifier columns
            sensitive_attributes: Sensitive columns to check
            
        Returns:
            Complete privacy audit report
        """
        logger.info("=" * 80)
        logger.info("COMPREHENSIVE PRIVACY AUDIT")
        logger.info("=" * 80)
        
        audit_results = {
            "timestamp": datetime.now().isoformat(),
            "record_count": len(df),
            "k_anonymity": self.check_k_anonymity(df, quasi_identifiers),
            "l_diversity": {},
            "t_closeness": {},
            "overall_privacy_score": 0
        }
        
        # Check L-diversity and T-closeness for each sensitive attribute
        for attr in sensitive_attributes:
            audit_results["l_diversity"][attr] = self.check_l_diversity(
                df, quasi_identifiers, attr
            )
            audit_results["t_closeness"][attr] = self.check_t_closeness(
                df, quasi_identifiers, attr
            )
        
        # Calculate overall privacy score (0-100)
        scores = []
        scores.append(100 if audit_results["k_anonymity"]["satisfies_k_anonymity"] else 0)
        
        for attr in sensitive_attributes:
            scores.append(100 if audit_results["l_diversity"][attr]["satisfies_l_diversity"] else 0)
            scores.append(100 if audit_results["t_closeness"][attr]["satisfies_t_closeness"] else 0)
        
        audit_results["overall_privacy_score"] = np.mean(scores)
        
        logger.info("=" * 80)
        logger.info(f"OVERALL PRIVACY SCORE: {audit_results['overall_privacy_score']:.1f}/100")
        logger.info("=" * 80)
        
        return audit_results
    
    def track_privacy_budget(self, query_name: str, epsilon: float):
        """
        Track differential privacy budget usage
        
        Args:
            query_name: Name of the query
            epsilon: Privacy cost of this query
        """
        self.privacy_budget["epsilon"] += epsilon
        self.privacy_budget["queries"].append({
            "query": query_name,
            "epsilon": epsilon,
            "timestamp": datetime.now().isoformat(),
            "cumulative_epsilon": self.privacy_budget["epsilon"]
        })
        
        logger.info(f"Privacy budget used: ε={epsilon:.4f}, "
                   f"Total: ε={self.privacy_budget['epsilon']:.4f}")
    
    def get_privacy_budget_report(self) -> Dict:
        """Get current privacy budget status"""
        return {
            "total_epsilon_used": self.privacy_budget["epsilon"],
            "total_queries": len(self.privacy_budget["queries"]),
            "queries": self.privacy_budget["queries"],
            "budget_remaining": max(0, 1.0 - self.privacy_budget["epsilon"]),
            "recommended_max": 1.0
        }


def main():
    """Test privacy engine with healthcare data"""
    logger.info("Testing Privacy Engine on Healthcare Data")
    
    # Connect to database
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="health_dw",
        user="user",
        password="pass"
    )
    
    # Load sample data
    query = """
    SELECT 
        p.age_group,
        p.gender,
        p.state,
        f.diagnosis,
        f.visit_type,
        f.cost
    FROM public.fact_visits f
    JOIN public.dim_patients p ON f.patient_key = p.patient_key
    LIMIT 5000
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"Loaded {len(df)} records for privacy analysis")
    
    # Initialize privacy engine
    engine = PrivacyEngine(k=5, l=3, t=0.2)
    
    # Define quasi-identifiers and sensitive attributes
    quasi_identifiers = ['age_group', 'gender', 'state']
    sensitive_attributes = ['diagnosis', 'visit_type']
    
    # Run comprehensive audit
    audit = engine.comprehensive_privacy_audit(
        df, quasi_identifiers, sensitive_attributes
    )
    
    # Print summary
    print("\n" + "=" * 80)
    print("PRIVACY AUDIT SUMMARY")
    print("=" * 80)
    print(f"Records analyzed: {audit['record_count']}")
    print(f"K-anonymity (k={engine.k}): {'✅ PASS' if audit['k_anonymity']['satisfies_k_anonymity'] else '❌ FAIL'}")
    print(f"  - Smallest group: {audit['k_anonymity']['smallest_group_size']}")
    print(f"  - Violating groups: {audit['k_anonymity']['violating_groups']}")
    
    for attr in sensitive_attributes:
        l_div = audit['l_diversity'][attr]
        t_close = audit['t_closeness'][attr]
        print(f"\n{attr.upper()}:")
        print(f"  L-diversity (l={engine.l}): {'✅ PASS' if l_div['satisfies_l_diversity'] else '❌ FAIL'}")
        print(f"    - Min diversity: {l_div['min_diversity']}")
        print(f"  T-closeness (t={engine.t}): {'✅ PASS' if t_close['satisfies_t_closeness'] else '❌ FAIL'}")
        print(f"    - Max distance: {t_close['max_distance']:.4f}")
    
    print(f"\n{'=' * 80}")
    print(f"OVERALL PRIVACY SCORE: {audit['overall_privacy_score']:.1f}/100")
    print(f"{'=' * 80}")
    
    # Test privacy budget tracking
    engine.track_privacy_budget("age_group_analysis", epsilon=0.1)
    engine.track_privacy_budget("diagnosis_distribution", epsilon=0.15)
    
    budget_report = engine.get_privacy_budget_report()
    print(f"\nPrivacy Budget Used: ε={budget_report['total_epsilon_used']:.4f}")
    print(f"Budget Remaining: ε={budget_report['budget_remaining']:.4f}")
    
    # Save audit results
    import json
    with open('privacy_audit_report.json', 'w') as f:
        json.dump(audit, f, indent=2, default=str)
    
    logger.info("Privacy audit complete! Report saved to privacy_audit_report.json")


if __name__ == "__main__":
    main()
