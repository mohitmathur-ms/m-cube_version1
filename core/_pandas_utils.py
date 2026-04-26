"""Small pandas utilities shared by core modules.

Right now this module exists for one purpose: ``iter_columns`` — a fast
column-major row iterator that replaces the ``DataFrame.iterrows()`` pattern
used across :mod:`core.backtest_runner` and :mod:`core.report_generator`.

``iterrows`` allocates a fresh ``Series`` per row, which dominates runtime on
the report-building paths (thousands of trades × dozens of columns). Pulling
each column out once with ``.tolist()`` and zipping the lists is ~5× faster
and produces identical output for the column accesses these callers do.
"""

from __future__ import annotations

from typing import Iterator

import pandas as pd


def iter_columns(df: pd.DataFrame, *names: str | None) -> Iterator[tuple]:
    """Iterate ``df`` row-by-row, yielding the values of ``names`` as a tuple.

    For each name in ``names``:

    * If it's ``None``, every yielded tuple slot for that position is ``None``.
      This matches the "column missing — substitute a fallback" pattern the
      report builders use after :func:`_resolve_column` returns ``None``.
    * Otherwise, the column is materialised once via ``.tolist()`` and walked
      positionally.

    Parameters
    ----------
    df :
        Source DataFrame. Iteration is positional (0..len(df)-1), independent
        of ``df.index``.
    names :
        Column names (or ``None`` for missing-column slots), in the order the
        consumer wants them in each yielded tuple.

    Yields
    ------
    tuple
        One tuple per row; same length as ``names``.
    """
    n = len(df)
    columns: list[list] = []
    for name in names:
        if name is None:
            columns.append([None] * n)
        else:
            columns.append(df[name].tolist())

    # zip(*columns) walks the column lists in lockstep, producing one tuple
    # per row. For very wide column lists this is still cheaper than
    # iterrows because no Series object is created.
    yield from zip(*columns)
