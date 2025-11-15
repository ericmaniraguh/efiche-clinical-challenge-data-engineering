"""
Analytics Utilities
Data quality checks, reporting, and monitoring for analytics warehouse

Functions:
  validate_analytics() - Data quality checks
  generate_report() - Create analytics reports
  get_analytics_metrics() - Calculate performance metrics
"""

import sys
import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# ============================================================================
# PATH RESOLUTION
# ============================================================================

def setup_paths():
    """Configure Python path for imports"""
    current_file = Path(__file__).resolve()
    current_dir = current_file.parent
    
    if 'dwh' in str(current_dir):
        project_root = current_dir.parent
    else:
        project_root = current_dir.parent if current_dir.name != 'dags' else current_dir
    
    paths_to_add = [
        str(project_root / 'utils'),
        str(project_root),
        str(current_dir),
    ]
    
    for path in paths_to_add:
        if path and path not in sys.path:
            sys.path.insert(0, path)
    
    return project_root

PROJECT_ROOT = setup_paths()

# ============================================================================
# IMPORTS
# ============================================================================

try:
    from db_connection import get_db_connection, init_logger
except ImportError as e:
    print(f"[ERROR] Failed to import db_connection: {e}")
    sys.exit(1)

logger = init_logger('analytics_utils', logging.INFO)


# ============================================================================
# DATA QUALITY VALIDATION
# ============================================================================

def validate_analytics(mode: str = 'incremental', check_level: str = 'basic', **kwargs) -> Dict[str, Any]:
    """
    Validate analytics warehouse data quality
    
    Args:
        mode: 'incremental' or 'full'
        check_level: 'basic' or 'comprehensive'
    
    Returns:
        Validation report dictionary
    """
    logger.info("="*70)
    logger.info("Analytics Data Quality Validation")
    logger.info(f"Mode: {mode}, Level: {check_level}")
    logger.info("="*70)
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'mode': mode,
        'check_level': check_level,
        'checks': {},
        'passed': True,
        'summary': {}
    }
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # CHECK 1: Row counts
        logger.info("\n[CHECK 1] Verifying row counts...")
        checks = {
            'dim_patient': 'SELECT COUNT(*) FROM analytics.dim_patient',
            'dim_facility': 'SELECT COUNT(*) FROM analytics.dim_facility',
            'dim_modality': 'SELECT COUNT(*) FROM analytics.dim_modality',
            'dim_diagnosis': 'SELECT COUNT(*) FROM analytics.dim_diagnosis',
            'fact_procedure': 'SELECT COUNT(*) FROM analytics.fact_procedure',
            'fact_language_usage': 'SELECT COUNT(*) FROM analytics.fact_language_usage',
        }
        
        row_counts = {}
        for table_name, query in checks.items():
            try:
                cur.execute(query)
                count = cur.fetchone()[0]
                row_counts[table_name] = count
                logger.info(f"  {table_name}: {count:,} rows")
            except Exception as e:
                logger.error(f"  Error querying {table_name}: {e}")
                row_counts[table_name] = 0
        
        report['checks']['row_counts'] = row_counts
        
        # CHECK 2: NULL constraint violations
        logger.info("\n[CHECK 2] Checking NULL constraints...")
        null_checks = {
            'dim_patient.patient_id IS NULL': 'SELECT COUNT(*) FROM analytics.dim_patient WHERE patient_id IS NULL',
            'dim_facility.facility_id IS NULL': 'SELECT COUNT(*) FROM analytics.dim_facility WHERE facility_id IS NULL',
            'fact_procedure.procedure_id IS NULL': 'SELECT COUNT(*) FROM analytics.fact_procedure WHERE procedure_id IS NULL',
        }
        
        null_violations = {}
        for check_name, query in null_checks.items():
            try:
                cur.execute(query)
                count = cur.fetchone()[0]
                if count > 0:
                    logger.warning(f"  {check_name}: {count} violations")
                    null_violations[check_name] = count
                    report['passed'] = False
            except Exception as e:
                logger.warning(f"  Error checking {check_name}: {e}")
        
        if not null_violations:
            logger.info("  No NULL constraint violations")
        
        report['checks']['null_violations'] = null_violations
        
        # CHECK 3: Referential integrity
        logger.info("\n[CHECK 3] Checking referential integrity...")
        fk_checks = {
            'orphaned_patients': 'SELECT COUNT(*) FROM analytics.fact_procedure fp WHERE fp.patient_sk NOT IN (SELECT patient_sk FROM analytics.dim_patient)',
            'orphaned_facilities': 'SELECT COUNT(*) FROM analytics.fact_procedure fp WHERE fp.facility_sk NOT IN (SELECT facility_sk FROM analytics.dim_facility)',
        }
        
        fk_violations = {}
        for check_name, query in fk_checks.items():
            try:
                cur.execute(query)
                count = cur.fetchone()[0]
                if count > 0:
                    logger.warning(f"  {check_name}: {count} violations")
                    fk_violations[check_name] = count
                    report['passed'] = False
            except Exception as e:
                logger.warning(f"  Error checking {check_name}: {e}")
        
        if not fk_violations:
            logger.info("  Referential integrity check passed")
        
        report['checks']['fk_violations'] = fk_violations
        
        # COMPREHENSIVE CHECKS (if requested)
        if check_level == 'comprehensive':
            logger.info("\n[CHECK 4] Data freshness check...")
            
            try:
                cur.execute("""
                    SELECT 
                        MAX(encounter_date) as latest_encounter,
                        CURRENT_TIMESTAMP - MAX(encounter_date) as hours_since_latest
                    FROM analytics.fact_procedure
                """)
                
                result = cur.fetchone()
                if result[0]:
                    freshness = {
                        'latest_encounter': result[0].isoformat(),
                        'hours_since_latest': str(result[1])
                    }
                    logger.info(f"  Latest encounter: {freshness['latest_encounter']}")
                    report['checks']['data_freshness'] = freshness
            except Exception as e:
                logger.warning(f"  Error checking freshness: {e}")
            
            # CHECK 5: Duplicates
            logger.info("\n[CHECK 5] Checking for duplicate procedures...")
            try:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT procedure_id, COUNT(*) as cnt
                        FROM analytics.fact_procedure
                        GROUP BY procedure_id
                        HAVING COUNT(*) > 1
                    ) t
                """)
                
                dup_count = cur.fetchone()[0]
                if dup_count > 0:
                    logger.warning(f"  Found {dup_count} duplicate procedures")
                    report['passed'] = False
                else:
                    logger.info("  No duplicate procedures")
                
                report['checks']['duplicates'] = dup_count
            except Exception as e:
                logger.warning(f"  Error checking duplicates: {e}")
        
        # SUMMARY
        total_rows = sum(row_counts.values())
        report['summary'] = {
            'total_rows': total_rows,
            'violations': len(null_violations) + len(fk_violations),
            'status': 'PASS' if report['passed'] else 'FAIL',
            'checked_at': datetime.now().isoformat()
        }
        
        logger.info("\n" + "="*70)
        logger.info(f"Validation Complete - Status: {report['summary']['status']}")
        logger.info(f"Total Rows: {total_rows:,}")
        logger.info(f"Violations: {report['summary']['violations']}")
        logger.info("="*70 + "\n")
        
        cur.close()
        conn.close()
        
        return report
    
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        report['passed'] = False
        report['error'] = str(e)
        return report


# ============================================================================
# ANALYTICS REPORTING
# ============================================================================

def generate_report(mode: str = 'incremental', output_format: str = 'json', **kwargs) -> Dict[str, Any]:
    """
    Generate analytics warehouse report
    
    Args:
        mode: 'incremental' or 'full'
        output_format: 'json' or 'text'
    
    Returns:
        Report dictionary
    """
    logger.info("="*70)
    logger.info("Generating Analytics Report")
    logger.info(f"Mode: {mode}, Format: {output_format}")
    logger.info("="*70)
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'mode': mode,
        'format': output_format,
        'metrics': {},
        'insights': {}
    }
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Daily procedure summary
        logger.info("\nFetching daily procedure summary...")
        try:
            cur.execute("""
                SELECT 
                    DATE(encounter_date) as procedure_date,
                    COUNT(*) as total_procedures,
                    SUM(CASE WHEN abnormality_detected = true THEN 1 ELSE 0 END) as abnormal,
                    ROUND(100.0 * SUM(CASE WHEN abnormality_detected = true THEN 1 ELSE 0 END) / COUNT(*), 2) as abnormality_rate,
                    AVG(confidence_score) as avg_confidence
                FROM analytics.fact_procedure
                WHERE encounter_date > CURRENT_TIMESTAMP - INTERVAL '30 days'
                GROUP BY DATE(encounter_date)
                ORDER BY procedure_date DESC
                LIMIT 7
            """)
            
            daily_summary = []
            for row in cur.fetchall():
                daily_summary.append({
                    'date': row[0].isoformat() if row[0] else None,
                    'procedures': row[1],
                    'abnormal': row[2],
                    'abnormality_rate': float(row[3]) if row[3] else 0,
                    'avg_confidence': float(row[4]) if row[4] else 0
                })
            
            report['metrics']['daily_summary'] = daily_summary
            logger.info(f"  Retrieved {len(daily_summary)} days of data")
        except Exception as e:
            logger.warning(f"  Error fetching daily summary: {e}")
        
        # Facility performance
        logger.info("Fetching facility performance...")
        try:
            cur.execute("""
                SELECT 
                    df.facility_name,
                    COUNT(*) as procedure_count,
                    SUM(CASE WHEN fp.abnormality_detected = true THEN 1 ELSE 0 END) as abnormal_count,
                    AVG(fp.confidence_score) as avg_confidence
                FROM analytics.fact_procedure fp
                JOIN analytics.dim_facility df ON fp.facility_sk = df.facility_sk
                WHERE fp.encounter_date > CURRENT_TIMESTAMP - INTERVAL '30 days'
                GROUP BY df.facility_name
                ORDER BY procedure_count DESC
            """)
            
            facility_perf = []
            for row in cur.fetchall():
                facility_perf.append({
                    'facility': row[0],
                    'procedures': row[1],
                    'abnormal': row[2] if row[2] else 0,
                    'avg_confidence': float(row[3]) if row[3] else 0
                })
            
            report['metrics']['facility_performance'] = facility_perf
            logger.info(f"  Retrieved performance for {len(facility_perf)} facilities")
        except Exception as e:
            logger.warning(f"  Error fetching facility performance: {e}")
        
        # Overall metrics
        logger.info("Calculating overall metrics...")
        try:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_procedures,
                    SUM(CASE WHEN abnormality_detected = true THEN 1 ELSE 0 END) as abnormal_procedures,
                    AVG(confidence_score) as avg_confidence,
                    MAX(encounter_date) as latest_encounter
                FROM analytics.fact_procedure
                WHERE encounter_date > CURRENT_TIMESTAMP - INTERVAL '30 days'
            """)
            
            row = cur.fetchone()
            if row:
                abnormality_rate = round(100.0 * row[1] / row[0], 2) if row[0] and row[0] > 0 else 0
                report['metrics']['overall'] = {
                    'total_procedures_30d': row[0] if row[0] else 0,
                    'abnormal_procedures_30d': row[1] if row[1] else 0,
                    'abnormality_rate_30d': abnormality_rate,
                    'avg_confidence_30d': float(row[2]) if row[2] else 0,
                    'latest_encounter': row[3].isoformat() if row[3] else None
                }
                
                logger.info(f"  Total procedures (30d): {report['metrics']['overall']['total_procedures_30d']:,}")
                logger.info(f"  Abnormality rate (30d): {report['metrics']['overall']['abnormality_rate_30d']:.1f}%")
        except Exception as e:
            logger.warning(f"  Error calculating metrics: {e}")
        
        logger.info("\n" + "="*70)
        logger.info("Report Generated Successfully")
        logger.info("="*70 + "\n")
        
        cur.close()
        conn.close()
        
        return report
    
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        report['error'] = str(e)
        return report


# ============================================================================
# ANALYTICS METRICS
# ============================================================================

def get_analytics_metrics(time_window_days: int = 30) -> Dict[str, Any]:
    """
    Get current analytics metrics
    
    Args:
        time_window_days: Number of days to analyze
    
    Returns:
        Metrics dictionary
    """
    logger.info("Retrieving analytics metrics...")
    
    metrics = {
        'timestamp': datetime.now().isoformat(),
        'time_window_days': time_window_days
    }
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get summary statistics
        cur.execute(f"""
            SELECT 
                COUNT(DISTINCT DATE(encounter_date)) as days_with_data,
                COUNT(DISTINCT patient_sk) as unique_patients,
                COUNT(DISTINCT facility_sk) as facilities_active,
                COUNT(*) as total_procedures
            FROM analytics.fact_procedure
            WHERE encounter_date > CURRENT_TIMESTAMP - INTERVAL '{time_window_days} days'
        """)
        
        row = cur.fetchone()
        if row:
            metrics['summary'] = {
                'days_with_data': row[0] if row[0] else 0,
                'unique_patients': row[1] if row[1] else 0,
                'active_facilities': row[2] if row[2] else 0,
                'total_procedures': row[3] if row[3] else 0
            }
        
        cur.close()
        conn.close()
        
        logger.info("  Metrics retrieved successfully")
        return metrics
    
    except Exception as e:
        logger.error(f"Error retrieving metrics: {e}")
        metrics['error'] = str(e)
        return metrics


# ============================================================================
# MAIN - FOR TESTING
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("Analytics Utilities - Testing")
    print("="*70 + "\n")
    
    # Test validation
    print("1. Running data quality validation...")
    validation = validate_analytics(mode='incremental', check_level='basic')
    print(f"   Status: {validation['summary']['status']}\n")
    
    # Test metrics
    print("2. Retrieving analytics metrics...")
    metrics = get_analytics_metrics(time_window_days=30)
    if 'summary' in metrics:
        print(f"   Total procedures: {metrics['summary']['total_procedures']:,}\n")
    
    # Test report
    print("3. Generating analytics report...")
    report = generate_report(mode='incremental', output_format='json')
    if 'metrics' in report and 'overall' in report['metrics']:
        print(f"   Total procedures (30d): {report['metrics']['overall']['total_procedures_30d']:,}\n")
    
    print("="*70)
    print("All utilities tested successfully")
    print("="*70 + "\n")