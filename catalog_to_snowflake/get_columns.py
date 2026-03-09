#!/usr/bin/env python3
"""
Module to fetch columns from Coalesce Catalog
"""

import logging
from typing import List, Dict
from .catalog_api_client import CatalogAPIClient

logger = logging.getLogger(__name__)


def fetch_columns_for_table(client: CatalogAPIClient, table_id: str, table_name: str = "") -> List[Dict]:
    """
    Fetch all columns for a specific table

    Args:
        client: CatalogAPIClient instance
        table_id: Table ID to fetch columns for
        table_name: Table name for logging purposes

    Returns:
        List of column objects with tags
    """
    query = """
    query GetColumnsByTableId($tableIds: [String!], $page: Int!, $pageSize: Int!) {
        getColumns(
            scope: {
                tableIds: $tableIds
                withHidden: false
            }
            pagination: {
                nbPerPage: $pageSize
                page: $page
            }
        ) {
            totalCount
            data {
                id
                name
                tableId
                createdAt
                updatedAt
                deletedAt
                tagEntities {
                    id
                    origin
                    createdAt
                    updatedAt
                    tag {
                        id
                        label
                    }
                }
            }
        }
    }
    """

    all_columns = []
    page = 0
    page_size = 10000

    while True:
        variables = {
            "tableIds": [table_id],
            "page": page,
            "pageSize": page_size
        }

        try:
            result = client.execute_query(query, variables)
            columns_data = result.get("data", {}).get("getColumns", {})
            columns = columns_data.get("data", [])
            total_count = columns_data.get("totalCount", 0)

            if page == 0:
                logger.info(f"  Fetching {total_count} columns for table: {table_name}")

            all_columns.extend(columns)

            # Check if we got all columns
            if len(columns) < page_size or len(all_columns) >= total_count:
                break

            page += 1

        except Exception as e:
            logger.error(f"  Failed to fetch columns for table {table_id}: {e}")
            break

    # Filter for columns with tags
    columns_with_tags = [
        col for col in all_columns
        if col.get("tagEntities") and len(col.get("tagEntities", [])) > 0
    ]

    if columns_with_tags:
        logger.info(f"    ✓ Found {len(columns_with_tags)} columns with tags")

    return columns_with_tags


def fetch_columns_for_tables_batch(client: CatalogAPIClient, table_ids: List[str], table_names_map: Dict[str, str] = None) -> Dict[str, List[Dict]]:
    """
    Fetch columns for multiple tables in a single batch request

    Args:
        client: CatalogAPIClient instance
        table_ids: List of table IDs to fetch columns for
        table_names_map: Optional mapping of table IDs to full names for logging

    Returns:
        Dictionary mapping table IDs to their columns with tags
    """
    if not table_ids:
        return {}

    query = """
    query GetColumnsByTableIds($tableIds: [String!], $page: Int!, $pageSize: Int!) {
        getColumns(
            scope: {
                tableIds: $tableIds
                withHidden: false
            }
            pagination: {
                nbPerPage: $pageSize
                page: $page
            }
        ) {
            totalCount
            data {
                id
                name
                tableId
                createdAt
                updatedAt
                deletedAt
                tagEntities {
                    id
                    origin
                    createdAt
                    updatedAt
                    tag {
                        id
                        label
                    }
                }
            }
        }
    }
    """

    all_columns = []
    page = 0
    page_size = 10000 # Large page size for maximum efficiency

    table_names_str = f" for {len(table_ids)} tables"
    logger.info(f"Fetching columns{table_names_str} in batch...")

    while True:
        variables = {
            "tableIds": table_ids,
            "page": page,
            "pageSize": page_size
        }

        try:
            result = client.execute_query(query, variables)
            columns_data = result.get("data", {}).get("getColumns", {})
            columns = columns_data.get("data", [])
            total_count = columns_data.get("totalCount", 0)

            if page == 0:
                logger.info(f"  Total columns to fetch: {total_count}")

            all_columns.extend(columns)

            # Check if we got all columns
            if len(columns) < page_size or len(all_columns) >= total_count:
                break

            page += 1

        except Exception as e:
            logger.error(f"  Failed to fetch columns batch: {e}")
            return {}

    # Group columns by table ID and filter for columns with tags
    columns_by_table = {}
    for col in all_columns:
        table_id = col.get("tableId")
        if table_id and col.get("tagEntities") and len(col.get("tagEntities", [])) > 0:
            if table_id not in columns_by_table:
                columns_by_table[table_id] = []
            columns_by_table[table_id].append(col)

    # Log results
    logger.info(f"  ✓ Fetched {len(all_columns)} total columns")
    logger.info(f"  ✓ Found {sum(len(cols) for cols in columns_by_table.values())} columns with tags across {len(columns_by_table)} tables")

    if table_names_map:
        for table_id, columns in columns_by_table.items():
            if table_id in table_names_map:
                logger.info(f"    • {table_names_map[table_id]}: {len(columns)} tagged columns")

    return columns_by_table


def process_tables_for_columns(client: CatalogAPIClient, tables: List[Dict], table_ids: List[str] = None,
                              limit: int = 10, batch_size: int = 1000, use_batch: bool = True) -> Dict[str, List[Dict]]:
    """
    Process multiple tables and fetch their columns

    Args:
        client: CatalogAPIClient instance
        tables: List of table objects
        table_ids: Optional list of specific table IDs to process
        limit: Maximum number of tables to process
        batch_size: Number of tables to process in each batch (default: 1000)
        use_batch: Whether to use batch fetching (default: True)

    Returns:
        Dictionary mapping table IDs to their columns with tags
    """
    # Filter tables if specific IDs provided
    if table_ids:
        tables_to_process = [t for t in tables if t.get("id") in table_ids]

        # If some table IDs weren't found in the fetched tables, still process them directly
        found_ids = [t.get("id") for t in tables_to_process]
        missing_ids = [tid for tid in table_ids if tid not in found_ids]

        if missing_ids:
            logger.info(f"Table ID(s) not in fetched Snowflake tables: {', '.join(missing_ids)}")
            logger.info("Will attempt to fetch columns directly for these tables...")

            # Create placeholder table entries for missing IDs
            for table_id in missing_ids:
                tables_to_process.append({
                    "id": table_id,
                    "name": f"Table_{table_id[:8]}",  # Use first 8 chars as name placeholder
                    "schema": {"name": "unknown", "database": {"name": "unknown"}}
                })

        if not tables_to_process:
            logger.warning("No tables to process")
            return {}
    else:
        tables_to_process = tables[:limit]

    if not tables_to_process:
        logger.warning("No tables to process")
        return {}

    logger.info(f"Processing {len(tables_to_process)} tables...")

    catalog_columns = {}

    if use_batch:
        # Create table ID to full name mapping for logging
        table_names_map = {}
        table_map = {}
        for table in tables_to_process:
            table_id = table.get("id")
            schema = table.get("schema", {})
            database = schema.get("database", {})
            full_name = f"{database.get('name')}.{schema.get('name')}.{table.get('name')}"
            table_names_map[table_id] = full_name
            table_map[table_id] = table

        # Process in batches
        for i in range(0, len(tables_to_process), batch_size):
            batch_tables = tables_to_process[i:i + batch_size]
            batch_ids = [t.get("id") for t in batch_tables]

            logger.info("")
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(tables_to_process) + batch_size - 1)//batch_size} ({len(batch_ids)} tables)...")

            # Fetch columns for batch
            columns_by_table = fetch_columns_for_tables_batch(client, batch_ids, table_names_map)

            # Build result structure - include tables with column tags
            for table_id, columns in columns_by_table.items():
                if table_id in table_map:
                    catalog_columns[table_id] = {
                        "table": table_map[table_id],
                        "columns": columns
                    }

            # Also include tables that have table-level tags but no column tags
            for table in batch_tables:
                table_id = table.get("id")
                if table_id not in catalog_columns:
                    table_tag_entities = table.get("tagEntities", [])
                    if table_tag_entities:
                        catalog_columns[table_id] = {
                            "table": table,
                            "columns": []
                        }
                        logger.info(f"    + {table_names_map.get(table_id, table_id)}: {len(table_tag_entities)} table-level tags (no column tags)")
    else:
        # Use original sequential processing
        for i, table in enumerate(tables_to_process, 1):
            table_id = table.get("id")
            table_name = table.get("name")
            schema = table.get("schema", {})
            database = schema.get("database", {})
            full_name = f"{database.get('name')}.{schema.get('name')}.{table_name}"

            logger.info("")
            logger.info(f"[{i}/{len(tables_to_process)}] Processing: {full_name}")

            # Get columns with tags
            columns_with_tags = fetch_columns_for_table(client, table_id, full_name)

            if columns_with_tags:
                catalog_columns[table_id] = {
                    "table": table,
                    "columns": columns_with_tags
                }
            elif table.get("tagEntities"):
                # Include tables with table-level tags even if no column tags
                catalog_columns[table_id] = {
                    "table": table,
                    "columns": []
                }
                logger.info(f"    + Table has {len(table.get('tagEntities', []))} table-level tags (no column tags)")

    return catalog_columns