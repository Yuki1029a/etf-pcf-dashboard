"""
先物銘柄名の正規化パーサー

PCFデータには79種類以上の先物銘柄名パターンが存在する。
このモジュールは全パターンを正規化し、種別・限月を抽出する。

限月表記パターン:
  - "2603"          → YYMM (年2桁+月2桁)
  - "MAR.2026"      → 英語月名.西暦4桁
  - "MAR 26"        → 英語月名 年2桁
  - "202603"        → 西暦4桁+月2桁
"""
from __future__ import annotations

import re
import logging
from typing import Optional

from config import FUTURES_MULTIPLIERS
from models import FuturesPosition

logger = logging.getLogger(__name__)

# ============================================================
# 月名マッピング
# ============================================================
MONTH_ABBR_TO_NUM = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}

# ============================================================
# 先物銘柄名の正規化ルール
# 順序が重要: より具体的なパターンを先に定義する
# ============================================================
# 各タプル: (コンパイル済み正規表現, 正規化後のfutures_type)
FUTURES_PATTERNS: list[tuple[re.Pattern, str]] = [
    # --- NK225 オプション (最優先: 最も具体的) ---
    (re.compile(r"NK225\s*\.OP\.CALL", re.IGNORECASE), "NK225_OPTION_CALL"),
    (re.compile(r"NK225\s*\.OP\.PUT", re.IGNORECASE), "NK225_OPTION_PUT"),

    # --- NK225 マイクロ ---
    (re.compile(r"(?:NK225|NIKKEI\s*225?)\s*MICRO", re.IGNORECASE), "NK225_MICRO"),

    # --- NK225 ミニ ---
    (re.compile(r"225\s*-?\s*MINI", re.IGNORECASE), "NK225_MINI"),
    (re.compile(r"NIKKEI\s*225?\s*MINI", re.IGNORECASE), "NK225_MINI"),
    (re.compile(r"NK225\s*MINI", re.IGNORECASE), "NK225_MINI"),
    (re.compile(r"MINI\s*-?\s*(?:NK|NIKKEI)\s*225", re.IGNORECASE), "NK225_MINI"),

    # --- NK225 (ラージ) ---
    (re.compile(r"NK225", re.IGNORECASE), "NK225"),
    (re.compile(r"NIKKEI\s*225", re.IGNORECASE), "NK225"),

    # --- ミニTOPIX ---
    (re.compile(r"MINI\s*-?\s*(?:TOPIX|TPX)", re.IGNORECASE), "MINI_TOPIX"),
    (re.compile(r"TOPIX\s*(?:INDX\s*)?MINI", re.IGNORECASE), "MINI_TOPIX"),

    # --- TOPIX Banks Index ---
    (re.compile(r"TOPIX\s*BANKS?\s*(?:INDEX)?", re.IGNORECASE), "TOPIX_BANKS"),

    # --- TOPIX Core30 ---
    (re.compile(r"TOPIX\s*CORE\s*30", re.IGNORECASE), "TOPIX_CORE30"),

    # --- TOPIX (ラージ) - Banks/Core30/Mini の後に配置 ---
    (re.compile(r"TOPIX", re.IGNORECASE), "TOPIX"),

    # --- JPX日経400 ---
    (re.compile(r"(?:JPX\s*-?\s*NIKKEI\s*(?:INDEX\s*)?400|NK400|JPXNIKKEI\s*400)", re.IGNORECASE), "JPX400"),

    # --- JPX Prime 150 ---
    (re.compile(r"JPX\s*PRIME\s*150", re.IGNORECASE), "JPX_PRIME150"),

    # --- 東証REIT ---
    (re.compile(r"(?:TSE\s*-?\s*REIT|TSEREIT|TOPIX\s*REIT)", re.IGNORECASE), "TSEREIT"),

    # --- TSE Growth ---
    (re.compile(r"TSE\s*GROWTH", re.IGNORECASE), "TSE_GROWTH"),

    # --- 10年国債先物 ---
    (re.compile(r"10\s*YEAR\s*JGB", re.IGNORECASE), "JGB10Y"),
]

# ============================================================
# 限月抽出パターン
# ============================================================
# パターン1: YYMM (4桁数字) - 例: "2603"
RE_CONTRACT_YYMM = re.compile(r"\b(\d{4})\b")

# パターン2: MMM.YYYY or MMM YYYY - 例: "MAR.2026", "MAR 2026"
RE_CONTRACT_MMM_YYYY = re.compile(
    r"\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[.\s](\d{4})\b",
    re.IGNORECASE,
)

# パターン3: MMM YY - 例: "MAR 26"
RE_CONTRACT_MMM_YY = re.compile(
    r"\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{2})\b",
    re.IGNORECASE,
)

# パターン4: YYYYMM (6桁) - 例: "202603", "NIKKEI225FUTUERS202603"
# \b では英字→数字の遷移を境界と認識しないため、(?<!\d)...(?!\d) を使用
RE_CONTRACT_YYYYMM = re.compile(r"(?<!\d)(20\d{4})(?!\d)")

# パターン5: オプション限月 - 例: ".FEB.2026."
RE_OPTION_MONTH = re.compile(
    r"\.(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\.(\d{4})\.",
    re.IGNORECASE,
)


def _extract_contract_month(raw_name: str) -> Optional[str]:
    """
    先物銘柄名から限月を YYMM 形式で抽出する。

    Returns:
        "2603" 等のYYMM文字列。抽出できない場合は None。
    """
    # オプション限月
    m = RE_OPTION_MONTH.search(raw_name)
    if m:
        month_str = MONTH_ABBR_TO_NUM[m.group(1).upper()]
        year_str = m.group(2)[2:]  # "2026" -> "26"
        return year_str + month_str

    # MMM.YYYY / MMM YYYY
    m = RE_CONTRACT_MMM_YYYY.search(raw_name)
    if m:
        month_str = MONTH_ABBR_TO_NUM[m.group(1).upper()]
        year_str = m.group(2)[2:]  # "2026" -> "26"
        return year_str + month_str

    # MMM YY
    m = RE_CONTRACT_MMM_YY.search(raw_name)
    if m:
        month_str = MONTH_ABBR_TO_NUM[m.group(1).upper()]
        year_str = m.group(2)
        return year_str + month_str

    # YYYYMM (6桁)
    m = RE_CONTRACT_YYYYMM.search(raw_name)
    if m:
        yyyymm = m.group(1)
        return yyyymm[2:]  # "202603" -> "2603"

    # YYMM (4桁) - 最後にチェック（誤検出リスクがあるため）
    # ただし、年は20-30の範囲、月は01-12の範囲に限定
    candidates = RE_CONTRACT_YYMM.findall(raw_name)
    for c in candidates:
        yy = int(c[:2])
        mm = int(c[2:])
        if 20 <= yy <= 35 and 1 <= mm <= 12:
            return c

    return None


def _classify_futures_type(raw_name: str) -> str:
    """
    先物銘柄名を正規化された種別に分類する。

    Returns:
        "TOPIX", "NK225", "NK225_MINI" 等の正規化種別。
        マッチしない場合は "UNKNOWN"。
    """
    # 文字化け対応: 先頭の化け文字を除去
    cleaned = re.sub(r"[・ｽ]+", "", raw_name).strip()

    for pattern, futures_type in FUTURES_PATTERNS:
        if pattern.search(cleaned):
            return futures_type

    logger.warning(f"未知の先物銘柄名: '{raw_name}'")
    return "UNKNOWN"


def normalize_futures(
    raw_name: str,
    quantity: int = 0,
    market_value: float = 0.0,
) -> FuturesPosition:
    """
    先物銘柄名を正規化し、FuturesPosition を返す。

    Args:
        raw_name: 生データの銘柄名 (例: "TOPIX 2603", "NK225 FUTURES MAR.2026")
        quantity: 建玉枚数 (正=買い, 負=売り)
        market_value: 時価評価額

    Returns:
        正規化された FuturesPosition
    """
    if not raw_name or not isinstance(raw_name, str):
        return FuturesPosition(
            raw_name=str(raw_name) if raw_name else "",
            futures_type="UNKNOWN",
            contract_month=None,
            quantity=quantity,
            market_value=market_value,
            multiplier=1,
        )

    raw_name = raw_name.strip()
    futures_type = _classify_futures_type(raw_name)
    contract_month = _extract_contract_month(raw_name)
    multiplier = FUTURES_MULTIPLIERS.get(futures_type, 1)

    return FuturesPosition(
        raw_name=raw_name,
        futures_type=futures_type,
        contract_month=contract_month,
        quantity=quantity,
        market_value=market_value,
        multiplier=multiplier,
    )


def get_all_futures_types() -> list[str]:
    """定義されている全先物種別を返す"""
    return list(FUTURES_MULTIPLIERS.keys())
