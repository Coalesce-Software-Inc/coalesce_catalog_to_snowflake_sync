#!/usr/bin/env python3
"""
Module to fetch Snowflake warehouse IDs from Coalesce Catalog
"""

import logging
from typing import List, Dict
from .catalog_api_client import CatalogAPIClient

logger = logging.getLogger(__name__)


def get_snowflake_warehouse_ids(client: CatalogAPIClient) -> List[str]:
    """
    Get Snowflake warehouse IDs from sources

    Args:
        client: CatalogAPIClient instance

    Returns:
        List of Snowflake warehouse IDs
    """
    query = """
    query GetSources {
        getSources {
            data {
                id
                name
                technology
                type
            }
        }
    }
    """

    try:
        result = client.execute_query(query)
        sources = result.get("data", {}).get("getSources", {}).get("data", [])

        # Filter for Snowflake sources and get their IDs
        snowflake_sources = [
            source for source in sources
            if source.get("technology") == "SNOWFLAKE"
        ]

        snowflake_ids = [source.get("id") for source in snowflake_sources]

        if snowflake_ids:
            logger.info(f"Found {len(snowflake_ids)} Snowflake warehouse(s)")
            for source in snowflake_sources:
                logger.info(f"  - {source.get('name')} (ID: {source.get('id')})")

        return snowflake_ids

    except Exception as e:
        logger.error(f"Failed to fetch Snowflake sources: {e}")
        return []