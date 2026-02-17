"""
日経平均・TOPIXの指数データ取得（Stooq CSV API）

Stooq から TOPIX指数（^TPX）と日経225（^NKX）を直接取得する。
yfinance では ^TPX が取得不可のため、Stooq を使用。
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

# Stooq CSV API
_STOOQ_URL = "https://stooq.com/q/d/l/?s={symbol}&d1={d1}&d2={d2}&i=d"
_TOPIX_SYMBOL = "^tpx"
_NK225_SYMBOL = "^nkx"


def _download_stooq(symbol: str, date_from: date, date_to: date) -> pd.DataFrame:
    """Stooq CSV APIから指数の日次OHLCVを取得する"""
    d1 = date_from.strftime("%Y%m%d")
    d2 = date_to.strftime("%Y%m%d")
    url = _STOOQ_URL.format(symbol=symbol, d1=d1, d2=d2)

    try:
        df = pd.read_csv(url)
    except Exception as e:
        logger.warning(f"Stooq取得エラー ({symbol}): {e}")
        return pd.DataFrame()

    if df.empty or "Close" not in df.columns:
        logger.warning(f"{symbol}: データなし")
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["Date"])
    return df[["date", "Close"]].rename(columns={"Close": "value"})


def fetch_index_data(
    date_from: date,
    date_to: date,
) -> pd.DataFrame:
    """
    Stooqから日経平均とTOPIXの終値を取得する。

    Returns:
        DataFrame: date, 日経平均, TOPIX
    """
    result = pd.DataFrame()

    # 日経平均
    nk = _download_stooq(_NK225_SYMBOL, date_from, date_to)
    if not nk.empty:
        nk = nk.rename(columns={"value": "日経平均"})
        result = nk

    # TOPIX（指数そのもの）
    topix = _download_stooq(_TOPIX_SYMBOL, date_from, date_to)
    if not topix.empty:
        topix = topix.rename(columns={"value": "TOPIX"})
        if result.empty:
            result = topix
        else:
            result = result.merge(topix, on="date", how="outer")

    if not result.empty:
        result = result.sort_values("date").reset_index(drop=True)

    return result
