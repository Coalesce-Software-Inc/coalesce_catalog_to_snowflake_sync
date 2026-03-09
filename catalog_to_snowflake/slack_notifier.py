#!/usr/bin/env python3
"""
Sends Slack notification for Catalog to Snowflake sync status.
"""
import json
import logging
import urllib.request
import urllib.error
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def build_slack_payload(status: str, stats: Dict[str, Any],
                        duration: str = "", error: Optional[str] = None,
                        actions_url: Optional[str] = None) -> Dict:
    """Construct the Slack message payload."""
    if status == "success":
        header_text = "Catalog to Snowflake Sync Successful"
        color = "#36a64f"
    else:
        header_text = "Catalog to Snowflake Sync Failed"
        color = "#e01e5a"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": header_text, "emoji": True}
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Status:*\n{'Success' if status == 'success' else 'Failed'}"},
                {"type": "mrkdwn", "text": f"*Duration:*\n{duration or 'N/A'}"}
            ]
        }
    ]

    # Extraction stats
    summary_parts = []
    if stats.get('tables_found'):
        summary_parts.append(f"*{stats['tables_found']}* Tables found")
    if stats.get('tables_with_tags'):
        summary_parts.append(f"*{stats['tables_with_tags']}* Tables with tags")
    if stats.get('sql_statements'):
        summary_parts.append(f"*{stats['sql_statements']}* SQL statements")
    if stats.get('drop_statements'):
        summary_parts.append(f"*{stats['drop_statements']}* DROP statements")

    if summary_parts:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Results:* {' | '.join(summary_parts)}"}
        })

    # Tag change stats
    tag_lines = []
    if stats.get('new_tables', 0) > 0 or stats.get('new_columns', 0) > 0:
        tag_lines.append(f"NEW: {stats.get('new_tables', 0)} tables, {stats.get('new_columns', 0)} columns")
    if stats.get('modified_tables', 0) > 0 or stats.get('modified_columns', 0) > 0:
        tag_lines.append(f"MODIFIED: {stats.get('modified_tables', 0)} tables, {stats.get('modified_columns', 0)} columns")
    if stats.get('removed_table_tags', 0) > 0 or stats.get('removed_column_tags', 0) > 0:
        tag_lines.append(f"REMOVED: {stats.get('removed_table_tags', 0)} tables, {stats.get('removed_column_tags', 0)} columns")

    if tag_lines:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Tag Changes:*\n" + "\n".join(f"- {l}" for l in tag_lines)}
        })

    if status != "success" and error:
        err_display = error[:300] + "..." if len(error) > 300 else error
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Error:*\n```{err_display}```"}
        })

    if actions_url:
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Download the SQL files and run them in Snowflake to apply tags."
            },
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": "Download & Run in Snowflake"},
                "url": actions_url
            }
        })

    return {
        "text": f"Catalog to Snowflake Sync: {status}",
        "attachments": [{"color": color, "blocks": blocks}]
    }


def send_slack_notification(webhook_url: str, status: str,
                            stats: Dict[str, Any] = None,
                            duration: str = "",
                            error: Optional[str] = None,
                            actions_url: Optional[str] = None) -> bool:
    """
    Send a Slack notification via webhook.

    Args:
        webhook_url: Slack incoming webhook URL
        status: 'success' or 'failure'
        stats: Dictionary with sync statistics
        duration: Duration string (e.g., '02:35')
        error: Error message if status is failure
        actions_url: GitHub Actions run URL for downloading artifacts

    Returns:
        True if notification was sent successfully
    """
    if not webhook_url:
        logger.debug("No SLACK_WEBHOOK_URL configured, skipping notification")
        return False

    payload = build_slack_payload(status, stats or {}, duration, error, actions_url=actions_url)

    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'}
    )

    try:
        logger.info("Sending Slack notification...")
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
            logger.info("Slack notification sent")
            return True
    except urllib.error.HTTPError as e:
        error_msg = e.read().decode('utf-8')
        logger.warning(f"Slack API error {e.code}: {error_msg}")
        return False
    except Exception as e:
        logger.warning(f"Failed to send Slack notification: {e}")
        return False
