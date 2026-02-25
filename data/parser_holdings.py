"""
個別銘柄の保有残高パーサー

ICE / S&P Global の PCF CSVから個別株式の保有情報を抽出する。
先物行・キャッシュ行は除外し、株式のみを返す。
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)


def parse_holdings_ice(
    csv_text: str,
    etf_code: str,
) -> list[dict]:
    """
    ICE形式のCSVから個別株式の保有情報を抽出する。

    Returns:
        list of dict: [{
            "etf_code": str,
            "date": date,
            "stock_code": str,   # 証券コード
            "stock_name": str,   # 銘柄名
            "shares": int,       # 保有株数
            "price": float,      # 株価
            "market_value": float,  # 時価 (shares × price)
        }, ...]
    """
    if not csv_text or not csv_text.strip():
        return []

    lines = csv_text.strip().split("\n")
    if len(lines) < 4:
        return []

    # --- 行1: メタデータ値 ---
    meta_reader = csv.reader(io.StringIO(lines[1]))
    meta_row = next(meta_reader, [])
    if len(meta_row) < 5:
        return []

    pcf_date = _parse_date(meta_row[4])
    if pcf_date is None:
        pcf_date = date.today()

    # --- 行3: カラムヘッダー ---
    col_header_reader = csv.reader(io.StringIO(lines[3]))
    col_headers = next(col_header_reader, [])
    col_headers_lower = [h.strip().lower() for h in col_headers]

    shares_col = -1
    price_col = -1
    for idx, h in enumerate(col_headers_lower):
        if h in ("shares amount", "shares"):
            shares_col = idx
        elif h == "stock price":
            price_col = idx

    if shares_col < 0:
        shares_col = 5
    if price_col < 0:
        price_col = shares_col + 1

    # --- 行4以降: 保有銘柄 ---
    holdings = []
    reader = csv.reader(io.StringIO("\n".join(lines[4:])))
    for row in reader:
        if len(row) < 3:
            continue

        # 先物行をスキップ
        if _is_futures_row(row):
            continue

        code_val = row[0].strip() if row[0] else ""
        name = row[1].strip() if len(row) > 1 else ""
        shares = _parse_int(row[shares_col]) if len(row) > shares_col else 0
        price = _parse_number(row[price_col]) if len(row) > price_col else None

        if not shares or not price:
            continue
        if not code_val and not name:
            continue

        holdings.append({
            "etf_code": etf_code,
            "date": pcf_date,
            "stock_code": code_val,
            "stock_name": name,
            "shares": shares,
            "price": price,
            "market_value": shares * price,
        })

    return holdings


def parse_holdings_spglobal(
    csv_text: str,
    etf_code: str,
) -> list[dict]:
    """
    S&P Global形式のCSVから個別株式の保有情報を抽出する。
    """
    if not csv_text or not csv_text.strip():
        return []

    lines = csv_text.strip().split("\n")
    if len(lines) < 4:
        return []

    # フォーマット判定
    header_line = lines[0].strip()
    is_amova = "Cash & Others" in header_line or "AUM" in header_line

    # --- 行1: メタデータ ---
    meta_reader = csv.reader(io.StringIO(lines[1]))
    meta_row = next(meta_reader, [])
    if len(meta_row) < 5:
        return []

    pcf_date = _parse_date(meta_row[4])
    if pcf_date is None:
        pcf_date = date.today()

    # --- 行3: カラムヘッダー ---
    col_header_reader = csv.reader(io.StringIO(lines[3]))
    col_headers = next(col_header_reader, [])
    col_headers_lower = [h.strip().lower() for h in col_headers]

    shares_col = -1
    price_col = -1
    mv_col = -1
    for idx, h in enumerate(col_headers_lower):
        if h in ("shares amount", "shares"):
            shares_col = idx
        elif h == "stock price":
            price_col = idx
        elif h == "market value":
            mv_col = idx

    if shares_col < 0:
        shares_col = 5
    if price_col < 0:
        price_col = shares_col + 1

    # --- 行4以降: 保有銘柄 ---
    holdings = []
    reader = csv.reader(io.StringIO("\n".join(lines[4:])))
    for row in reader:
        if len(row) < 3:
            continue

        code_val = row[0].strip() if row[0] else ""
        name = row[1].strip() if len(row) > 1 else ""

        # Cash / Margin行をスキップ（アモーヴァ形式）
        if code_val.lower() in ("cash", "margin"):
            continue

        # 先物行をスキップ
        if _is_futures_row(row):
            continue

        shares = _parse_int(row[shares_col]) if len(row) > shares_col else 0
        price = _parse_number(row[price_col]) if len(row) > price_col else None

        if not shares or not price:
            continue
        if not code_val and not name:
            continue

        # market_value: 専用列があればそれ、なければ計算
        if mv_col >= 0 and len(row) > mv_col and row[mv_col].strip():
            market_val = _parse_number(row[mv_col]) or (shares * price)
        else:
            market_val = shares * price

        holdings.append({
            "etf_code": etf_code,
            "date": pcf_date,
            "stock_code": code_val,
            "stock_name": name,
            "shares": shares,
            "price": price,
            "market_value": market_val,
        })

    return holdings


def parse_holdings(
    csv_text: str,
    etf_code: str,
    provider: str = "ice",
) -> list[dict]:
    """プロバイダに応じたホールディングスパーサー"""
    try:
        if provider == "ice":
            return parse_holdings_ice(csv_text, etf_code)
        elif provider == "spglobal":
            return parse_holdings_spglobal(csv_text, etf_code)
        else:
            return []
    except Exception as e:
        logger.error(f"Holdings parse error ({provider}, {etf_code}): {e}")
        return []


# ============================================================
# ヘルパー関数（parser_pcf.py と共通）
# ============================================================
def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip().strip('"')
    for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d", "%d-%b-%Y"]:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_number(s) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip().strip('"').replace(",", "")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int(s) -> Optional[int]:
    f = _parse_number(s)
    return int(f) if f is not None else None


def _is_futures_row(row: list[str]) -> bool:
    """先物行かどうか判定（parser_pcf.pyと同一ロジック）"""
    import re
    if len(row) < 3:
        return False

    code = row[0].strip() if row[0] else ""
    name = (row[1].strip() if len(row) > 1 else "").upper()

    exchange = ""
    for idx in [3, 4]:
        if len(row) > idx and row[idx]:
            val = row[idx].strip().upper()
            if val in ("OSE", "XOSE", "TSE", "XTKS", "SAP", "OTC", "HKF", "TOCOM",
                       "XNYS", "XNAS"):
                exchange = val
                break

    if exchange in ("OSE", "XOSE"):
        if not code:
            return True
        if re.match(r"^[A-Z]{2,4}[A-Z0-9]\d$", code):
            return True

    if not code and name:
        futures_name_patterns = [
            r"FUTURES", r"FUTR",
            r"TOPIX\s+\d{4}", r"NK225\s+\d{4}",
            r"NIKKEI\s*225?\s+\d", r"TOPIX\s+INDX",
            r"NIKKEI\s+225\s+MINI", r"JGB", r"先物",
        ]
        for pat in futures_name_patterns:
            if re.search(pat, name):
                return True

    return False
