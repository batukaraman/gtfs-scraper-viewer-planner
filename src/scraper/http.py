"""HTTP client for the EasyWay AJAX API."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import requests


def easyway_request(
    endpoint: str,
    *,
    headers: Dict[str, str],
    logger: logging.Logger,
    params: Optional[Dict] = None,
    data: Optional[Dict] = None,
    form_data: Optional[Dict] = None,
    timeout: int = 30,
) -> Any:
    """GET/POST JSON. Returns parsed JSON or None on failure."""
    try:
        if form_data:
            response = requests.post(endpoint, headers=headers, data=form_data, timeout=timeout)
        elif data:
            response = requests.post(endpoint, headers=headers, json=data, timeout=timeout)
        else:
            response = requests.get(endpoint, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error("Request failed (%s): %s", endpoint, e)
        return None
