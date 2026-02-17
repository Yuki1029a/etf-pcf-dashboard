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

    ICE形式のCSVは固定位置フォーマット:
      行0: ETF Code,ETF Name,Fund Cash Component,Shares Outstanding,Fund Date
      行1: 1306,TOPIX ETF,478108071923.0000,8131915510,20260217
      行2: (空行)
      行3: Code,Name,ISIN,Exchange,Currency,Shares Amount,Stock Price
      行4~: 保有銘柄データ
    """
    if not csv_text or not csv_text.strip():
        return None

    lines = csv_text.strip().split("\n")
    if len(lines) < 4:
        return None

    # --- 行1: メタデータ値 ---
    meta_reader = csv.reader(io.StringIO(lines[1]))
    meta_row = next(meta_reader, [])

    if len(meta_row) < 5:
        logger.warning(f"ICE メタデータ不足: {etf_code}")
        return None

    cash = _parse_number(meta_row[2])
    shares_outstanding = _parse_int(meta_row[3])
    pcf_date = _parse_date(meta_row[4])
    if pcf_date is None:
        pcf_date = date.today()

    # --- 行3: カラムヘッダー ---
    col_header_reader = csv.reader(io.StringIO(lines[3]))
    col_headers = next(col_header_reader, [])
    col_headers_lower = [h.strip().lower() for h in col_headers]

    # カラム位置を特定
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
    futures_positions = []
    total_equity_value = 0.0
    total_equity_count = 0

    reader = csv.reader(io.StringIO("\n".join(lines[4:])))
    for row in reader:
        if len(row) < 3:
            continue

        name = row[1].strip() if len(row) > 1 else ""

        if _is_futures_row(row):
            quantity = _parse_int(row[shares_col]) if len(row) > shares_col else 0
            price = _parse_number(row[price_col]) if len(row) > price_col else 0.0
            market_val = (quantity or 0) * (price or 0)
            fp = normalize_futures(name, quantity or 0, market_val)
            futures_positions.append(fp)
        else:
            shares = _parse_int(row[shares_col]) if len(row) > shares_col else 0
            price = _parse_number(row[price_col]) if len(row) > price_col else None
            if shares and price:
                total_equity_value += shares * price
                total_equity_count += shares

    # NAV: cash + equity で算出
    nav = None
    if cash is not None or total_equity_value > 0:
        nav = (cash or 0) + total_equity_value

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
# S&P Global パーサー
# ============================================================
def parse_spglobal_pcf(csv_text: str, etf_code: str) -> Optional[PCFRecord]:
    """
    S&P Global形式のPCF CSVをパースする。

    2つのサブフォーマットに対応:
      フォーマットA (大和/農林中金/ニッセイ): ICE形式と同一ヘッダー
        行0: ETF Code,ETF Name,Fund Cash Component,Shares Outstanding,Fund Date,,
        行3: Code,Name,ISIN,Exchange,Currency,Shares Amount,Stock Price

      フォーマットB (アモーヴァ): 拡張形式
        行0: ETF Code,ETF Name,Cash & Others,Shares Outstanding,Fund Date,AUM
        行3: Code,Name,Isin,Exchange,Currency,Shares,Stock Price,Market Value,FX Rate,...,Future multiplier
    """
    if not csv_text or not csv_text.strip():
        return None

    lines = csv_text.strip().split("\n")
    if len(lines) < 4:
        return None

    # フォーマット判定: 行0のヘッダーで区別
    header_line = lines[0].strip()
    is_amova = "Cash & Others" in header_line or "AUM" in header_line

    # --- 行0: ヘッダー名 ---
    # --- 行1: メタデータ値 ---
    meta_reader = csv.reader(io.StringIO(lines[1]))
    meta_row = next(meta_reader, [])

    if len(meta_row) < 5:
        logger.warning(f"S&P Global メタデータ不足: {etf_code}")
        return None

    cash = _parse_number(meta_row[2])
    shares_outstanding = _parse_int(meta_row[3])
    pcf_date = _parse_date(meta_row[4])
    if pcf_date is None:
        pcf_date = date.today()

    # AUM (アモーヴァ形式のみ)
    nav = None
    if is_amova and len(meta_row) > 5:
        nav = _parse_number(meta_row[5])

    # --- 行3: カラムヘッダー ---
    col_header_reader = csv.reader(io.StringIO(lines[3]))
    col_headers = next(col_header_reader, [])
    col_headers_lower = [h.strip().lower() for h in col_headers]

    # カラム位置を特定
    shares_col = -1
    price_col = -1
    mv_col = -1
    multiplier_col = -1
    for idx, h in enumerate(col_headers_lower):
        if h in ("shares amount", "shares"):
            shares_col = idx
        elif h == "stock price":
            price_col = idx
        elif h == "market value":
            mv_col = idx
        elif h == "future multiplier":
            multiplier_col = idx

    if shares_col < 0:
        # フォールバック: 5列目or6列目
        shares_col = 5
    if price_col < 0:
        price_col = shares_col + 1

    # --- 行4以降: 保有銘柄 ---
    futures_positions = []
    total_equity_value = 0.0
    total_equity_count = 0

    reader = csv.reader(io.StringIO("\n".join(lines[4:])))
    for row in reader:
        if len(row) < 3:
            continue

        code_val = row[0].strip() if row[0] else ""
        name = row[1].strip() if len(row) > 1 else ""

        # Cash / Margin 行をスキップ (アモーヴァ形式)
        if code_val.lower() in ("cash", "margin"):
            continue

        # 先物判定
        if _is_futures_row(row):
            quantity = _parse_int(row[shares_col]) if len(row) > shares_col else 0
            price = _parse_number(row[price_col]) if len(row) > price_col else 0.0

            # market_value: 専用列があればそれを使用、なければ計算
            if mv_col >= 0 and len(row) > mv_col and row[mv_col].strip():
                market_val = _parse_number(row[mv_col]) or 0.0
            else:
                market_val = (quantity or 0) * (price or 0)

            # 先物名: アモーヴァ形式はName列、大和形式もName列
            fp = normalize_futures(name, quantity or 0, market_val)
            futures_positions.append(fp)
        else:
            # 現物株
            shares = _parse_int(row[shares_col]) if len(row) > shares_col else 0
            price = _parse_number(row[price_col]) if len(row) > price_col else None
            if shares and price:
                total_equity_value += shares * price
                total_equity_count += shares

    # NAV計算: アモーヴァ形式はAUM列、大和形式は cash + equity で概算
    if nav is None and (cash is not None or total_equity_value > 0):
        nav = (cash or 0) + total_equity_value

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
        elif provider == "spglobal":
            return parse_spglobal_pcf(csv_text, etf_code)
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
    """
    CSVの行が先物データかどうかを判定する。

    先物行の特徴:
      - Exchange列が OSE / XOSE（先物取引所）
      - Code列が空（ICE/大和形式）かつ Name列に先物キーワード+限月を含む
      - Code列が先物コード（TPH6, NKH6 等 - アモーヴァ形式）
      - 銘柄名に "FUTURES" "FUTR" + 限月表記を含む
    """
    if len(row) < 3:
        return False

    code = row[0].strip() if row[0] else ""
    name = (row[1].strip() if len(row) > 1 else "").upper()

    # Exchange列の位置を推定 (3番目 or 4番目)
    exchange = ""
    for idx in [3, 4]:
        if len(row) > idx and row[idx]:
            val = row[idx].strip().upper()
            if val in ("OSE", "XOSE", "TSE", "XTKS", "SAP", "OTC", "HKF", "TOCOM",
                       "XNYS", "XNAS"):
                exchange = val
                break

    # OSE/XOSE は先物取引所 (ほぼ確実に先物)
    if exchange in ("OSE", "XOSE"):
        # ただし 現物株がOSEに上場している場合がある
        # Code列が空、または先物コードパターンならば先物
        if not code:
            return True
        # アモーヴァ形式の先物コード (TPH6, NKH6, NOH6 等 - 2-4文字+1-2数字)
        if re.match(r"^[A-Z]{2,4}[A-Z0-9]\d$", code):
            return True

    # Code列が空で Name列に先物キーワードを含む
    if not code and name:
        futures_name_patterns = [
            r"FUTURES",
            r"FUTR",
            r"TOPIX\s+\d{4}",       # "TOPIX 2603"
            r"NK225\s+\d{4}",       # "NK225 2603"
            r"NIKKEI\s*225?\s+\d",  # "NIKKEI 225 2603"
            r"TOPIX\s+INDX",        # "TOPIX INDX FUTR"
            r"NIKKEI\s+225\s+MINI", # "NIKKEI 225 MINI 2603"
            r"JGB",
            r"先物",
        ]
        for pat in futures_name_patterns:
            if re.search(pat, name):
                return True

    return False
