#!/usr/bin/env python3
"""
Module to save all output files (JSON, SQL, reports)
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def save_json_data(data: Any, filepath: Path, description: str = "data") -> bool:
    """
    Save data to JSON file

    Args:
        data: Data to save
        filepath: Path to save file
        description: Description for logging

    Returns:
        Success status
    """
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"✓ {description} saved to: {filepath}")
        return True
    except Exception as e:
        logger.error(f"Failed to save {description}: {e}")
        return False


def save_results(
    snowflake_tables: List[Dict],
    catalog_columns: Dict[str, Dict],
    sql_statements: str,
    output_dir: str = "data",
    sql_dir: str = "sql",
    reports_dir: str = "reports",
    unified_sql_content: str = "",
    tag_stats: Dict = None
) -> Dict[str, str]:
    """
    Save all results to files

    Args:
        snowflake_tables: List of Snowflake tables
        catalog_columns: Dictionary of table columns with tags
        sql_statements: Generated SQL statements
        output_dir: Directory for data files
        sql_dir: Directory for SQL files
        reports_dir: Directory for report files
        unified_sql_content: Optional unified change SQL (DROP + SET statements)
        tag_stats: Optional dictionary with tag change statistics

    Returns:
        Dictionary with file paths
    """
    # Create directories if they don't exist
    Path(output_dir).mkdir(exist_ok=True)
    Path(sql_dir).mkdir(exist_ok=True)
    Path(reports_dir).mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_files = {}

    # Save Snowflake tables data
    if snowflake_tables:
        tables_file = Path(output_dir) / f"snowflake_tables_{timestamp}.json"
        tables_data = {
            "timestamp": datetime.now().isoformat(),
            "total_count": len(snowflake_tables),
            "tables": snowflake_tables
        }
        if save_json_data(tables_data, tables_file, "Snowflake tables"):
            output_files["tables_file"] = str(tables_file)

    # Save catalog tables and columns data
    if catalog_columns:
        tables_columns_file = Path(output_dir) / f"catalog_tables_columns_{timestamp}.json"

        # Save the full structure including table metadata for DROP TAG comparison
        tables_columns_data = {
            "timestamp": datetime.now().isoformat(),
            "tables_with_columns": len(catalog_columns),
            "catalog_tables_columns": catalog_columns  # Save the full structure (tables + columns)
        }
        if save_json_data(tables_columns_data, tables_columns_file, "Catalog tables and columns"):
            output_files["tables_columns_file"] = str(tables_columns_file)

    # Save SQL statements
    if sql_statements:
        sql_file = Path(sql_dir) / f"complete_current_state_{timestamp}.sql"
        try:
            with open(sql_file, 'w', encoding='utf-8') as f:
                f.write(sql_statements)
            logger.info(f"✓ Complete current state SQL saved to: {sql_file}")
            output_files["sql_file"] = str(sql_file)
        except Exception as e:
            logger.error(f"Failed to save SQL file: {e}")

    # Save unified change SQL file if provided
    if unified_sql_content:
        unified_sql_file = Path(sql_dir) / f"changes_since_last_full_run_{timestamp}.sql"
        try:
            with open(unified_sql_file, 'w', encoding='utf-8') as f:
                f.write(unified_sql_content)
            logger.info(f"✓ Changes since last full run saved to: {unified_sql_file}")
            output_files["unified_sql_file"] = str(unified_sql_file)
        except Exception as e:
            logger.error(f"Failed to save changes SQL file: {e}")

    # Save summary report
    report_file = Path(reports_dir) / f"sync_report_{timestamp}.txt"
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("Coalesce Catalog to Snowflake Sync Report\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n\n")
            f.write(f"Tables found: {len(snowflake_tables)}\n")
            f.write(f"Tables with tagged columns: {len(catalog_columns)}\n")

            # Count SQL statements
            if sql_statements:
                sql_lines = sql_statements.split('\n')
                sql_count = len([s for s in sql_lines if s.strip() and not s.startswith('--')])
                f.write(f"SQL statements generated: {sql_count}\n")

            # Add tag statistics if provided
            if tag_stats:
                f.write("\nTag Change Statistics:\n")
                f.write("-" * 40 + "\n")
                f.write(f"New tables with tags: {tag_stats.get('new_tables', 0)}\n")
                f.write(f"New columns with tags: {tag_stats.get('new_columns', 0)}\n")
                f.write(f"Modified tables: {tag_stats.get('modified_tables', 0)}\n")
                f.write(f"Modified columns: {tag_stats.get('modified_columns', 0)}\n")
                f.write(f"Tables with removed tags: {tag_stats.get('removed_table_tags', 0)}\n")
                f.write(f"Columns with removed tags: {tag_stats.get('removed_column_tags', 0)}\n")

            # Count statements in unified file if provided
            if unified_sql_content:
                unified_lines = unified_sql_content.split('\n')
                drop_count = len([s for s in unified_lines if "UNSET TAG" in s])
                set_count = len([s for s in unified_lines if "SET TAG" in s])
                f.write(f"DROP TAG statements in unified file: {drop_count}\n")
                f.write(f"SET TAG statements in unified file: {set_count}\n")

            f.write("\n")

            if catalog_columns:
                f.write("Tables with tags:\n")
                f.write("-" * 40 + "\n")
                for table_id, table_data in catalog_columns.items():
                    table = table_data.get("table", {})
                    columns = table_data.get("columns", [])
                    schema = table.get("schema", {})
                    database = schema.get("database", {})
                    full_name = f"{database.get('name')}.{schema.get('name')}.{table.get('name')}"
                    f.write(f"• {full_name}\n")
                    f.write(f"  Columns with tags: {len(columns)}\n")

        logger.info(f"✓ Summary report saved to: {report_file}")
        output_files["report_file"] = str(report_file)

    except Exception as e:
        logger.error(f"Failed to save report: {e}")

    return output_files