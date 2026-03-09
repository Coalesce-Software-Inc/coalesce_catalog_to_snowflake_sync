"""
Modules for Catalog to Snowflake Tag Synchronization

This package contains modular components for:
- API client management
- Fetching warehouses, tables, and columns
- Generating SQL statements
- Saving output files
"""

from .catalog_api_client import CatalogAPIClient
from .get_warehouses import get_snowflake_warehouse_ids
from .get_tables import get_all_snowflake_tables, fetch_snowflake_tables, fetch_table_by_id
from .get_columns import fetch_columns_for_table, process_tables_for_columns
from .generate_sql import generate_all_sql_statements, create_sql_file_content
from .compute_changes import process_drop_tags, process_tag_changes, create_new_tags_sql, generate_unified_change_sql
from .save_outputs import save_results
from .slack_notifier import send_slack_notification

__all__ = [
    'CatalogAPIClient',
    'get_snowflake_warehouse_ids',
    'get_all_snowflake_tables',
    'fetch_snowflake_tables',
    'fetch_table_by_id',
    'fetch_columns_for_table',
    'process_tables_for_columns',
    'generate_all_sql_statements',
    'create_sql_file_content',
    'process_drop_tags',
    'process_tag_changes',
    'create_new_tags_sql',
    'generate_unified_change_sql',
    'save_results',
    'send_slack_notification'
]