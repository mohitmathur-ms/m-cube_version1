"""Ad-hoc validation: re-run CRYPTO_REEXEC_ENTRY and inspect the spliced
orderbook for the original (pre-clip) SOLUSD entry that the ReExecute captured.

Run: venv\\Scripts\\python.exe scripts\\_validate_reexec_orderbook.py
"""
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from core.models.persistence import load_portfolio
from core.backtest_runner import run_portfolio_backtest
from core.report_generator import build_orderbook_dataframe

CATALOG = str(PROJECT_DIR / "catalog")
USER = "_default"


def main():
    import pandas as pd
    pf = load_portfolio("_default/CRYPTO_REEXEC_ENTRY", directory=str(PROJECT_DIR / "portfolios"))
    results = run_portfolio_backtest(catalog_path=CATALOG, portfolio=pf, user_id=USER)

    print("=" * 70)
    print("pf_reexec_replays:", results.get("pf_reexec_replays"))
    print("total_pnl:", results.get("total_pnl"))
    print("total_trades:", results.get("total_trades"))

    ob = build_orderbook_dataframe({"CRYPTO_REEXEC_ENTRY": results}, user_id=USER)
    cols = ["SYMBOL", "TRANSACTION", "ENTRY TIME", "ENTRY PRICE", "ENTRY REASON",
            "EXIT TIME", "AVG EXIT PRICE", "EXIT REASON", "PNL"]
    pd.set_option("display.width", 220)
    pd.set_option("display.max_colwidth", 38)
    pd.set_option("display.max_rows", 50)
    print("=" * 70)
    print(f"ORDERBOOK ({len(ob)} rows):")
    print(ob[cols].to_string())
    print("=" * 70)
    sol = ob[ob["SYMBOL"] == "SOLUSD"].sort_values("ENTRY TIME")
    print("SOLUSD rows:", len(sol))
    print(sol[cols].to_string())


if __name__ == "__main__":
    main()
