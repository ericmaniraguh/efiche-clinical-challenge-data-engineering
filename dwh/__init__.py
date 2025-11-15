"""
eFICHE Analytics Pipeline Package
Automated data warehouse population and management

Modules:
  - etl_analytics: Main ETL engine
  - analytics_utils: Validation and reporting utilities
  - db_connection: Database connection management

Usage:
    from analytics_pipeline.etl_analytics import populate_analytics_warehouse
    
    stats = populate_analytics_warehouse(mode='incremental')
"""

__version__ = '1.0.0'
__author__ = 'Data Engineering Team'

from .etl_analytics import (
    AnalyticsETL,
    populate_analytics_warehouse
)

from .analytics_utils import (
    validate_analytics,
    generate_report,
    get_analytics_metrics
)

__all__ = [
    'AnalyticsETL',
    'populate_analytics_warehouse',
    'validate_analytics',
    'generate_report',
    'get_analytics_metrics',
]