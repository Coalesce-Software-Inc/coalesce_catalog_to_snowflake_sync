#!/usr/bin/env python3
"""
Module to generate Snowflake ALTER statements from catalog data
"""

import logging
from typing import Dict, List, Tuple, Set
from datetime import datetime

logger = logging.getLogger(__name__)


def parse_tag_label(tag_label: str) -> Tuple[str, str]:
    """
    Parse tag label to extract key-value pair

    Args:
        tag_label: Tag label from catalog (e.g., "catalog:sensitive location")

    Returns:
        Tuple of (key, value) where key is uppercase and value preserves original case
    """
    if ":" in tag_label:
        # Split on first colon only to handle cases where value might contain colons
        parts = tag_label.split(":", 1)
        key = parts[0].strip().upper()
        value = parts[1].strip()
        return key, value
    else:
        # If no colon, use the whole label as both key and value
        clean_label = tag_label.strip()
        return clean_label.upper(), clean_label


def escape_sql_value(value: str) -> str:
    """
    Escape a string value for safe inclusion in Snowflake SQL.
    Replaces single quotes with doubled single quotes (Snowflake convention).

    Args:
        value: Raw string value

    Returns:
        Escaped string safe for SQL interpolation
    """
    if not value:
        return value
    return value.replace("'", "''")


def format_snowflake_identifier(name: str) -> str:
    """
    Format an identifier for Snowflake (uppercase and replace spaces with underscores)

    Args:
        name: Original name

    Returns:
        Formatted identifier suitable for Snowflake
    """
    return name.replace(" ", "_").replace("-", "_").upper()


def collect_all_tags(catalog_columns: Dict[str, Dict]) -> Set[Tuple[str, str]]:
    """
    Collect all unique tag key-value pairs from the catalog data

    Args:
        catalog_columns: Dictionary mapping table IDs to their table info and columns

    Returns:
        Set of unique (key, value) tuples
    """
    all_tags = set()

    for table_data in catalog_columns.values():
        # Table-level tags
        table_info = table_data.get("table", {})
        for tag_entity in table_info.get("tagEntities", []):
            tag_label = tag_entity.get("tag", {}).get("label", "")
            if tag_label:
                key, value = parse_tag_label(tag_label)
                all_tags.add((key, value))

        # Column-level tags
        for column in table_data.get("columns", []):
            for tag_entity in column.get("tagEntities", []):
                tag_label = tag_entity.get("tag", {}).get("label", "")
                if tag_label:
                    key, value = parse_tag_label(tag_label)
                    all_tags.add((key, value))

    return all_tags


def generate_create_tag_statements(all_tags: Set[Tuple[str, str]]) -> List[str]:
    """
    Generate CREATE TAG statements for all unique tag keys

    Args:
        all_tags: Set of unique (key, value) tuples

    Returns:
        List of CREATE TAG statements
    """
    statements = []

    # Group tags by key
    tag_keys = {}
    for key, value in all_tags:
        if key not in tag_keys:
            tag_keys[key] = []
        tag_keys[key].append(value)

    statements.append("-- ============================================================")
    statements.append("-- CREATE TAG statements")
    statements.append("-- These tags must exist in Snowflake before applying to objects")
    statements.append("-- ============================================================")
    statements.append("")

    for key in sorted(tag_keys.keys()):
        tag_name = format_snowflake_identifier(key)
        values = sorted(set(tag_keys[key]))

        statements.append(f"-- Tag: {tag_name}")
        statements.append(f"-- Values found in catalog: {', '.join(values)}")
        statements.append(f"CREATE TAG IF NOT EXISTS {tag_name}")
        statements.append(f"    COMMENT = 'Tag imported from catalog with key: {key}';")
        statements.append("")

    return statements


def format_timestamp_comment(timestamp_ms):
    """
    Convert millisecond timestamp to human-readable format for SQL comments

    Args:
        timestamp_ms: Timestamp in milliseconds since epoch

    Returns:
        Formatted string like "2026-02-08 15:16:34"
    """
    if not timestamp_ms:
        return ""

    try:
        # Convert milliseconds to seconds and create datetime
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        # Format as readable date/time
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        # If conversion fails, return the original value
        return str(timestamp_ms)


def generate_alter_statements_for_table(table_info: Dict, columns: List[Dict]) -> List[str]:
    """
    Generate ALTER statements for a single table and its columns

    Args:
        table_info: Table metadata (including table-level tags)
        columns: List of columns with tags

    Returns:
        List of SQL statements
    """
    statements = []

    schema = table_info.get("schema", {})
    database = schema.get("database", {})
    full_table_path = f"{database.get('name')}.{schema.get('name')}.{table_info.get('name')}"

    # Get table updatedAt timestamp
    table_updated_at = table_info.get("updatedAt", "")

    # Add header comment
    statements.append(f"-- Table: {full_table_path}")

    # Check for table-level tags
    table_tags_added = False
    table_tag_entities = table_info.get("tagEntities", [])
    if table_tag_entities:
        statements.append(f"-- Table-level tags: {len(table_tag_entities)}")
        table_tags_added = True

        # Process table-level tags
        for tag_entity in table_tag_entities:
            tag = tag_entity.get("tag", {})
            tag_label = tag.get("label", "")
            # Get both timestamps from the TagEntity itself
            tag_entity_created_at = tag_entity.get("createdAt", "")
            tag_entity_updated_at = tag_entity.get("updatedAt", "")

            if tag_label:
                key, value = parse_tag_label(tag_label)
                tag_name = format_snowflake_identifier(key)

                # Add timestamp comment if available - showing both TagEntity timestamps
                created_readable = format_timestamp_comment(tag_entity_created_at)
                updated_readable = format_timestamp_comment(tag_entity_updated_at)

                if created_readable and updated_readable:
                    # If both timestamps are the same (tag never modified), show simplified format
                    if created_readable == updated_readable:
                        timestamp_comment = f"  -- Tag applied: {created_readable}"
                    else:
                        timestamp_comment = f"  -- Tag applied: {created_readable}, last updated: {updated_readable}"
                elif created_readable:
                    timestamp_comment = f"  -- Tag applied: {created_readable}"
                elif updated_readable:
                    timestamp_comment = f"  -- Tag updated: {updated_readable}"
                else:
                    timestamp_comment = ""

                statements.append("")
                statements.append(f"-- Apply table-level tag: {tag_label}")
                statements.append(f"ALTER TABLE {full_table_path}")
                statements.append(f"    SET TAG {tag_name} = '{escape_sql_value(value)}';{timestamp_comment}")

    # Process column tags if any
    if columns:
        if table_tags_added:
            statements.append("")
        statements.append(f"-- Column-level tags: {len(columns)} columns with tags")
        statements.append("")

        # Process each column individually with its tags
        for column in columns:
            column_name = column.get("name", "")
            tag_entities = column.get("tagEntities", [])

            # Collect all tags for this column with their individual timestamps
            column_tags = []
            for tag_entity in tag_entities:
                tag = tag_entity.get("tag", {})
                tag_label = tag.get("label", "")
                # Get both timestamps from the TagEntity itself
                tag_entity_created_at = tag_entity.get("createdAt", "")
                tag_entity_updated_at = tag_entity.get("updatedAt", "")

                if tag_label:
                    key, value = parse_tag_label(tag_label)
                    tag_name = format_snowflake_identifier(key)
                    column_tags.append((tag_name, value, tag_label, tag_entity_created_at, tag_entity_updated_at))

            # Generate ALTER statement for each tag on this column
            if column_tags:
                for tag_name, value, original_label, tag_entity_created_at, tag_entity_updated_at in column_tags:
                    # Add timestamp comment if available - showing both TagEntity timestamps
                    created_readable = format_timestamp_comment(tag_entity_created_at)
                    updated_readable = format_timestamp_comment(tag_entity_updated_at)

                    if created_readable and updated_readable:
                        # If both timestamps are the same (tag never modified), show simplified format
                        if created_readable == updated_readable:
                            timestamp_comment = f"  -- Tag applied: {created_readable}"
                        else:
                            timestamp_comment = f"  -- Tag applied: {created_readable}, last updated: {updated_readable}"
                    elif created_readable:
                        timestamp_comment = f"  -- Tag applied: {created_readable}"
                    elif updated_readable:
                        timestamp_comment = f"  -- Tag updated: {updated_readable}"
                    else:
                        timestamp_comment = ""

                    statements.append(f"-- Column {column_name}: {original_label}")
                    statements.append(f"ALTER TABLE {full_table_path}")
                    statements.append(f"    ALTER COLUMN {column_name}")
                    statements.append(f"        SET TAG {tag_name} = '{escape_sql_value(value)}';{timestamp_comment}")
                statements.append("")
    elif not table_tags_added:
        # No tags at all
        return []

    return statements


def generate_all_sql_statements(catalog_columns: Dict[str, Dict]) -> List[str]:
    """
    Generate ALTER statements for all tables with tagged columns or table tags

    Args:
        catalog_columns: Dictionary mapping table IDs to their table info and columns

    Returns:
        List of all SQL statements
    """
    all_statements = []

    # First collect all unique tags
    all_tags = collect_all_tags(catalog_columns)

    # Generate CREATE TAG statements
    create_statements = generate_create_tag_statements(all_tags)
    all_statements.extend(create_statements)

    # Add separator
    all_statements.append("")
    all_statements.append("-- ============================================================")
    all_statements.append("-- ALTER TABLE statements to apply tags")
    all_statements.append("-- ============================================================")
    all_statements.append("")

    # Generate ALTER statements for each table
    for table_id, table_data in catalog_columns.items():
        table_info = table_data.get("table", {})
        columns = table_data.get("columns", [])

        statements = generate_alter_statements_for_table(table_info, columns)
        if statements:
            all_statements.extend(statements)
            all_statements.append("")  # Add blank line between tables

    # Count non-comment, non-empty statements
    sql_count = len([s for s in all_statements if s.strip() and not s.startswith('--')])
    logger.info(f"Generated {sql_count} SQL statements for {len(catalog_columns)} table(s)")
    logger.info(f"Found {len(all_tags)} unique tag key-value pairs")

    return all_statements


def create_sql_file_content(statements: List[str], catalog_columns: Dict[str, Dict]) -> str:
    """
    Create complete SQL file content with header and footer

    Args:
        statements: List of SQL statements
        catalog_columns: Dictionary with table and column data

    Returns:
        Complete SQL file content as string
    """
    # Collect all unique tags for summary
    all_tags = collect_all_tags(catalog_columns)

    # Group tags by key for summary
    tag_keys = {}
    for key, value in all_tags:
        if key not in tag_keys:
            tag_keys[key] = set()
        tag_keys[key].add(value)

    # Build header
    header = [
        "-- ============================================================",
        "-- Snowflake TAG Management Script",
        "-- Generated from Catalog Metadata",
        f"-- Generated: {datetime.now().isoformat()}",
        f"-- Tables processed: {len(catalog_columns)}",
        "-- ============================================================",
        "",
        "-- This script will:",
        "-- 1. Create all necessary tags in Snowflake",
        "-- 2. Apply tags to tables and columns based on catalog metadata",
        "",
        "-- IMPORTANT: Review and run this script in the appropriate Snowflake context",
        "-- You may need to set your database and schema context:",
        "-- USE DATABASE your_database;",
        "-- USE SCHEMA your_schema;",
        "",
        "-- ============================================================",
        ""
    ]

    # Build footer with summary
    footer = [
        "",
        "-- ============================================================",
        "-- Summary:",
        f"--   Tables processed: {len(catalog_columns)}",
        f"--   Unique tag keys: {len(tag_keys)}",
    ]

    # Add details about each tag key
    for key in sorted(tag_keys.keys()):
        values = sorted(tag_keys[key])
        footer.append(f"--   Tag '{key}': {len(values)} unique values")
        if len(values) <= 5:
            footer.append(f"--     Values: {', '.join(values)}")
        else:
            footer.append(f"--     Sample values: {', '.join(values[:3])}, ... ({len(values)} total)")

    footer.append(f"--   Total tag assignments: {sum(1 for _ in all_tags)}")
    footer.append("-- ============================================================")
    footer.append("-- End of script")

    # Combine all parts
    full_content = header + statements + footer

    return "\n".join(full_content)