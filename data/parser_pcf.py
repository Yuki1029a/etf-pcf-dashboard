"""
PCF CSVパーサー

3つのプロバイダ（ICE, Solactive, S&P Global）のCSVを
共通の PCFRecord / PCFHolding に変換する。
"""
from __future__ import annotations

import csv
import io
import re
import logging
from datetime import date, datetime
from typing import Optional

from models import PCFRecord, FuturesPosition
from data.parser_futures import normalize_futures

logger = logging.getLogger(__name__)


# ============================================================
# ICE Data Services パーサー
# ============================================================
def parse_ice_pcf(csv_text: str, etf_code: str) -> Optional[PCFRecord]:
    """
    ICE Data Services形式のPCF CSVをパースする。

    ICE形式のCSVは以下のセクション構成:
    - ヘッダーセクション (ETF情報、日付、NAV等)
    - 保有銘柄セクション (現物株、先物、現金等)
    """
    if not csv_text or not csv_text.strip():
        return None

    lines = csv_text.strip().split("\n")
    if len(lines) < 2:
        return None

    # メタデータ抽出
    meta = {}
    holdings_start = -1

    for i, line in enumerate(lines):
        line_stripped = line.strip()

        # 空行またはセパレータでセクション切替
        if not line_stripped:
            continue

        # キーバリューペアの検出
        if "," in line_stripped:
            parts = line_stripped.split(",", 1)
            key = parts[0].strip().strip('"')
            val = parts[1].strip().strip('"') if len(parts) > 1 else ""

            # 主要メタデータキーの検出
            key_lower = key.lower()
            if any(k in key_lower for k in ["fund code", "etf code", "security code"]):
                meta["code"] = val
            elif any(k in key_lower for k in ["fund date", "pcf date", "date"]):
                meta["date"] = val
            elif any(k in key_lower for k in ["nav", "net asset"]):
                meta["nav"] = val
            elif any(k in key_lower for k in ["shares outstanding", "units outstanding", "口数"]):
                meta["shares_outstanding"] = val
            elif any(k in key_lower for k in ["cash component", "cash", "現金"]):
                meta["cash"] = val

            # 保有銘柄ヘッダーの検出
            if any(k in key_lower for k in ["code", "銘柄コード", "security"]):
                if any(h in line_stripped.lower() for h in [
                    "shares", "amount", "株数", "quantity",
                ]):
                    holdings_start = i + 1

    # 日付の解析
    pcf_date = _parse_date(meta.get("date", ""))
    if pcf_date is None:
        pcf_date = date.today()

    # 数値の解析
    nav = _parse_number(meta.get("nav"))
    shares_outstanding = _parse_int(meta.get("shares_outstanding"))
    cash = _parse_number(meta.get("cash"))

    # 保有銘柄からの集計
    futures_positions = []
    total_equity_value = 0.0
    total_equity_count = 0

    if holdings_start > 0:
        reader = csv.reader(io.StringIO("\n".join(lines[holdings_start:])))
        for row in reader:
            if len(row) < 3:
                continue

            # 先物かどうかの判定
            name = row[1].strip() if len(row) > 1 else ""
            is_futures = _is_futures_row(row)

            if is_futures:
                quantity = _parse_int(row[2]) if len(row) > 2 else 0
                market_val = _parse_number(row[-1]) if row[-1].strip() else 0.0
                fp = normalize_futures(name, quantity or 0, market_val or 0.0)
                futures_positions.append(fp)
            else:
                # 現物株
                shares = _parse_int(row[2]) if len(row) > 2 else 0
                price = _parse_number(row[-2]) if len(row) > 3 else None
                if shares and price:
                    total_equity_value += shares * price
                    total_equity_count += shares

    record = PCFRecord(
        etf_code=etf_code,
        pcf_date=pcf_date,
        nav=nav,
        shares_outstanding=shares_outstanding,
        cash_component=cash,
        equity_count_tse=total_equity_count if total_equity_count else None,
        equity_market_value=total_equity_value if total_equity_value else None,
        futures_positions=futures_positions[:2],  # 最大2つ
    )

    return record


# ============================================================
# Solactive AG パーサー
# ============================================================
def parse_solactive_pcf(csv_text: str, etf_code: str) -> Optional[PCFRecord]:
    """
    Solactive AG形式のPCF CSVをパースする。

    Solactive形式:
      セクション1 (メタデータ): Code, Fund Name, Cash Component, Shares Outstanding, Fund Date
      セクション2 (保有銘柄): Code, Name, ISIN, Exchange, Currency, Shares Amount, Stock Price
    """
    if not csv_text or not csv_text.strip():
        return None

    lines = csv_text.strip().split("\n")

    # メタデータ抽出
    meta = {}
    holdings_start = -1
    section = 0

    for i, line in enumerate(lines):
        line_stripped = line.strip()
        if not line_stripped:
            section += 1
            continue

        parts = [p.strip().strip('"') for p in line_stripped.split(",")]

        if section == 0:
            # メタデータセクション
            if len(parts) >= 2:
                key = parts[0].lower()
                val = parts[1]
                if "code" in key and "code" not in meta:
                    meta["code"] = val
                elif "cash" in key:
                    meta["cash"] = val
                elif "shares outstanding" in key or "units" in key:
                    meta["shares_outstanding"] = val
                elif "date" in key:
                    meta["date"] = val
                elif "nav" in key:
                    meta["nav"] = val
        else:
            # 保有銘柄ヘッダーの検出
            if holdings_start < 0:
                if any("code" in p.lower() for p in parts):
                    holdings_start = i + 1
                    continue
            break

    pcf_date = _parse_date(meta.get("date", ""))
    if pcf_date is None:
        pcf_date = date.today()

    nav = _parse_number(meta.get("nav"))
    shares_outstanding = _parse_int(meta.get("shares_outstanding"))
    cash = _parse_number(meta.get("cash"))

    # 保有銘柄解析
    futures_positions = []
    total_equity_value = 0.0
    total_equity_count = 0

    if holdings_start > 0:
        reader = csv.reader(io.StringIO("\n".join(lines[holdings_start:])))
        for row in reader:
            if len(row) < 6:
                continue

            name = row[1].strip() if len(row) > 1 else ""
            shares_amount = _parse_int(row[5]) if len(row) > 5 else 0
            stock_price = _parse_number(row[6]) if len(row) > 6 else None

            if _is_futures_row(row):
                fp = normalize_futures(
                    name,
                    shares_amount or 0,
                    (shares_amount or 0) * (stock_price or 0),
                )
                futures_positions.append(fp)
            else:
                if shares_amount and stock_price:
                    total_equity_value += shares_amount * stock_price
                    total_equity_count += shares_amount

    record = PCFRecord(
        etf_code=etf_code,
        pcf_date=pcf_date,
        nav=nav,
        shares_outstanding=shares_outstanding,
        cash_component=cash,
        equity_count_tse=total_equity_count if total_equity_count else None,
        equity_market_value=total_equity_value if total_equity_value else None,
        futures_positions=futures_positions[:2],
    )

    return record


# ============================================================
# 統合パーサー
# ============================================================
def parse_pcf(
    csv_text: str,
    etf_code: str,
    provider: str = "ice",
) -> Optional[PCFRecord]:
    """
    プロバイダに応じたパーサーでPCF CSVをパースする。

    Args:
        csv_text: CSVテキスト
        etf_code: ETFコード
        provider: "ice", "solactive", "spglobal"

    Returns:
        PCFRecord。パース失敗時はNone。
    """
    try:
        if provider == "ice":
            return parse_ice_pcf(csv_text, etf_code)
        elif provider == "solactive":
            return parse_solactive_pcf(csv_text, etf_code)
        else:
            logger.warning(f"未対応プロバイダ: {provider}")
            return None
    except Exception as e:
        logger.error(f"PCFパースエラー ({provider}, {etf_code}): {e}")
        return None


# ============================================================
# ヘルパー関数
# ============================================================
def _parse_date(s: str) -> Optional[date]:
    """日付文字列をパース"""
    if not s:
        return None

    s = s.strip().strip('"')

    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y%m%d",
        "%d-%b-%Y",  # "12-Feb-2026"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_number(s: str | None) -> Optional[float]:
    """数値文字列をfloatに変換"""
    if s is None:
        return None
    s = str(s).strip().strip('"').replace(",", "")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int(s: str | None) -> Optional[int]:
    """数値文字列をintに変換"""
    f = _parse_number(s)
    if f is not None:
        return int(f)
    return None


def _is_futures_row(row: list[str]) -> bool:
    """CSVの行が先物データかどうかを判定"""
    row_text = " ".join(str(c) for c in row).upper()

    futures_keywords = [
        "FUTURES", "FUTURE", "FUTR",
        "TOPIX", "NK225", "NIKKEI",
        "MINI", "MICRO",
        "REIT.*FUTR", "JGB",
        "OPTION", ".OP.",
        "先物",
    ]

    for kw in futures_keywords:
        if re.search(kw, row_text):
            return True

    return False
