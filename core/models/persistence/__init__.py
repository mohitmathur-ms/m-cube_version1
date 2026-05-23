"""Portfolio file persistence (JSON on disk under ``portfolios/``).

Save / load / list / delete portfolios as ``<name>.json`` files. Built on
``serialization`` for the dict conversion; ``save_portfolio`` stamps
``updated_at`` before writing.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from core.models.portfolio_config import PortfolioConfig
from core.models.serialization import portfolio_from_dict, portfolio_to_dict


def save_portfolio(config: PortfolioConfig, directory: str = "portfolios") -> Path:
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / f"{config.name}.json"
    config.updated_at = datetime.now(timezone.utc).isoformat()
    with open(file_path, "w") as f:
        json.dump(portfolio_to_dict(config), f, indent=2)
    return file_path


def load_portfolio(name: str, directory: str = "portfolios") -> PortfolioConfig:
    file_path = Path(directory) / f"{name}.json"
    with open(file_path) as f:
        data = json.load(f)
    return portfolio_from_dict(data)


def list_portfolios(directory: str = "portfolios") -> list[str]:
    dir_path = Path(directory)
    if not dir_path.exists():
        return []
    return sorted(p.stem for p in dir_path.glob("*.json"))


def delete_portfolio(name: str, directory: str = "portfolios") -> bool:
    file_path = Path(directory) / f"{name}.json"
    if file_path.exists():
        file_path.unlink()
        return True
    return False
