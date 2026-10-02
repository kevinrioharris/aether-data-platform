"""Connector registry: built-in connectors plus third-party plugins.

A plugin package registers connectors through an entry point, with no change to this package:

    # the plugin's pyproject.toml
    [project.entry-points."dataplatform.connectors"]
    salesforce = "acme_connectors.salesforce:SalesforceConnector"
"""

from __future__ import annotations

import importlib
import logging
from importlib.metadata import entry_points
from typing import Any

from dataplatform.connectors.base import ConfigurationError, Connector

log = logging.getLogger(__name__)

ENTRY_POINT_GROUP = "dataplatform.connectors"
_BUILTIN_MODULES = ["dataplatform.connectors.files"]

_registry: dict[str, type[Connector]] = {}
_loaded = False


def register(cls: type[Connector]) -> type[Connector]:
    """Class decorator for connectors."""
    existing = _registry.get(cls.type)
    if existing is not None and existing is not cls:
        raise ValueError(f"connector type {cls.type!r} already registered by {existing.__module__}")
    _registry[cls.type] = cls
    return cls


def _load() -> None:
    global _loaded
    if _loaded:
        return
    _loaded = True
    for module in _BUILTIN_MODULES:
        importlib.import_module(module)
    for ep in entry_points(group=ENTRY_POINT_GROUP):
        try:
            register(ep.load())
        except Exception:  # a broken plugin must not take the platform down
            log.exception("failed to load connector plugin %s", ep.value)


def get(connector_type: str) -> type[Connector]:
    _load()
    try:
        return _registry[connector_type]
    except KeyError:
        raise ConfigurationError(f"unknown connector type {connector_type!r} (known: {sorted(_registry)})") from None


def available() -> list[type[Connector]]:
    _load()
    return [_registry[k] for k in sorted(_registry)]


def create(connector_type: str, config: dict[str, Any], **deps: Any) -> Connector:
    return get(connector_type).from_config(config, **deps)
