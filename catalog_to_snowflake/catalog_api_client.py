#!/usr/bin/env python3
"""
Base API Client for Coalesce Catalog GraphQL API
Provides core GraphQL query execution functionality
"""

import json
import logging
import requests
from typing import Dict, Any, Optional


class CatalogAPIClient:
    """Base client for Coalesce Catalog GraphQL API"""

    def __init__(self, api_token: str, api_url: str = "https://api.us.castordoc.com/public/graphql"):
        """Initialize the API client"""
        self.api_token = api_token
        self.api_url = api_url
        self.headers = {
            "Authorization": api_token,
            "Content-Type": "application/json"
        }
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute_query(self, query: str, variables: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute a GraphQL query"""
        payload = {
            "query": query,
            "variables": variables if variables else {}
        }

        try:
            response = requests.post(
                self.api_url,
                json=payload,
                headers=self.headers,
                timeout=300
            )

            # Try to get response data even on error
            data = {}
            try:
                data = response.json()
                if "errors" in data:
                    self.logger.error(f"GraphQL errors: {data['errors']}")
                    raise Exception(f"GraphQL query failed: {data['errors']}")
            except json.JSONDecodeError:
                self.logger.error(f"Response status: {response.status_code}")
                response.raise_for_status()

            response.raise_for_status()
            return data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed: {e}")
            raise