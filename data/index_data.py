"""
日経平均・TOPIXの指数データ取得（yfinance）

Note: ^TPX (TOPIX指数) はyfinanceで取得不可のため、
      1306.T (NEXT FUNDS TOPIX ETF) の終値を代替として使用する。
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

# TOPIX指数の代替: TOPIX連動ETF (1306.T)
# ^TPX はyfinanceで "possibly delisted" のため取得不可
_TOPIX_PROXY_TICKER = "1306.T"
_NK225_TICKER = "^N225"


def _download_ticker(ticker: str, start: str, end: str) -> pd.DataFrame:
    """yfinanceから1銘柄の終値を取得する"""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance がインストールされていません")
        return pd.DataFrame()

    try:
        data = yf.download(ticker, start=start, end=end, progress=False)
    except Exception as e:
        logger.warning(f"yfinance取得エラー ({ticker}): {e}")
        return pd.DataFrame()

    if data.empty:
        logger.warning(f"{ticker}: データなし")
        return pd.DataFrame()

    close = data["Close"].reset_index()
    # yfinance v0.2+: MultiIndex columns の場合
    if isinstance(close.columns, pd.MultiIndex):
        close.columns = ["date", "value"]
    else:
        close.columns = ["date", "value"]

    return close


def fetch_index_data(
    date_from: date,
    date_to: date,
) -> pd.DataFrame:
    """
    yfinanceから日経平均とTOPIX（代替ETF）の終値を取得する。

    Returns:
        DataFrame: date, 日経平均, TOPIX
    """
    start = date_from.strftime("%Y-%m-%d")
    end = (date_to + timedelta(days=1)).strftime("%Y-%m-%d")

    result = pd.DataFrame()

    # 日経平均
    nk = _download_ticker(_NK225_TICKER, start, end)
    if not nk.empty:
        nk = nk.rename(columns={"value": "日経平均"})
        result = nk

    # TOPIX (代替: 1306.T ETF終値)
    topix = _download_ticker(_TOPIX_PROXY_TICKER, start, end)
    if not topix.empty:
        topix = topix.rename(columns={"value": "TOPIX"})
        if result.empty:
            result = topix
        else:
            result = result.merge(topix, on="date", how="outer")

    if not result.empty:
        result["date"] = pd.to_datetime(result["date"])
        result = result.sort_values("date").reset_index(drop=True)

    return result
