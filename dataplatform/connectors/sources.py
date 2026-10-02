"""Source registry: everything declared in config/sources.yml."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class SourceConfig:
    name: str
    type: str  # a connector type ("excel", "csv", ...) or "rest_api"
    description: str = ""
    owner: str = ""
    options: dict[str, Any] = field(default_factory=dict)

    def __getitem__(self, key: str) -> Any:
        return self.options[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.options.get(key, default)


def load_sources(path: Path) -> dict[str, SourceConfig]:
    raw = yaml.safe_load(path.read_text())["sources"]
    sources = {}
    for name, cfg in raw.items():
        cfg = dict(cfg)
        sources[name] = SourceConfig(
            name=name,
            type=cfg.pop("type"),
            description=cfg.pop("description", ""),
            owner=cfg.pop("owner", ""),
            options=cfg,
        )
    return sources
