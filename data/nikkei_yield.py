"""
日経225採用銘柄の「予想配当利回り（日経予想）」取得

nikkei.com の銘柄ページ (nkd/company/?scode=XXXX) から日経記者予想ベースの
予想配当利回りを取得する。採用銘柄リストは 1321 (NF日経225) の最新PCF保有銘柄
（＝225銘柄）から導出するため、外部の構成銘柄リストは不要。

スロットリング: 1.3秒/銘柄 + 25銘柄ごとに25秒休止。スタブ応答（<50KB）は
ブロック扱いで40秒待って再試行（最大4回）。225銘柄で約8〜10分。
"""
from __future__ import annotations

import http.cookiejar
import logging
import re
import time
import urllib.request
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)

NIKKEI_WARMUP_URL = "https://www.nikkei.com/markets/kabu/"
NIKKEI_COMPANY_URL = "https://www.nikkei.com/nkd/company/?scode={code}"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.8",
    "Referer": NIKKEI_WARMUP_URL,
}
_STUB_MAX = 50_000       # 正常ページ ~180KB、スタブ/ブロック ~3KB
_SLEEP_PER_CODE = 1.3
_BATCH_SIZE = 25
_BATCH_COOLDOWN = 25
_STUB_RETRY_WAIT = 40
_STUB_MAX_TRIES = 4

CONSTITUENT_SOURCE_ETF = "1321"  # NF日経225 の保有銘柄 = 日経225採用銘柄


def get_nikkei225_constituents(holdings_df: pd.DataFrame) -> pd.DataFrame:
    """1321 の最新日の保有銘柄から日経225採用銘柄 (stock_code, stock_name) を返す"""
    h = holdings_df[holdings_df["etf_code"] == CONSTITUENT_SOURCE_ETF]
    if h.empty:
        return pd.DataFrame(columns=["stock_code", "stock_name"])
    latest = h["date"].max()
    h = h[h["date"] == latest]
    cons = (
        h[["stock_code", "stock_name"]]
        .astype({"stock_code": str})
        .drop_duplicates("stock_code")
        .sort_values("stock_code")
        .reset_index(drop=True)
    )
    logger.info(f"日経225採用銘柄: {len(cons)} 銘柄 (1321 保有 {latest.date()} 時点)")
    return cons


def _build_opener():
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    op.addheaders = list(_HEADERS.items())
    try:
        op.open(NIKKEI_WARMUP_URL, timeout=20).read()
    except Exception as e:
        logger.warning(f"nikkei.com ウォームアップ失敗: {e}")
    return op


def parse_nikkei_yield(html: str) -> tuple[float | None, str | None, str]:
    """
    ページHTMLから (予想配当利回り, 基準日 'YYYY/MM/DD', status) を返す。
    無配・未定は利回り None, status 'nodiv:<表示>'。
    """
    pos = [m.start() for m in re.finditer("予想配当利回り", html) if m.start() > 3000]
    if not pos:
        return None, None, "no_label"
    seg = html[pos[0]:pos[0] + 400]
    mv = re.search(r'm-stockInfo_detail_value">\s*([\d]+\.?\d*|-{1,3}|―+)', seg)
    if not mv:
        return None, None, "no_value"
    md = re.search(r"(\d{4}/\d{2}/\d{2})", seg)
    as_of = md.group(1) if md else None
    v = mv.group(1)
    try:
        return float(v), as_of, "ok"
    except ValueError:
        return None, as_of, f"nodiv:{v}"


def fetch_nikkei_yields(
    constituents: pd.DataFrame,
    fetch_date: date,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    採用銘柄の予想配当利回り（日経予想）を順次取得する。

    Returns:
        DataFrame: date, stock_code, stock_name, nikkei_yield, as_of, status
    """
    rows = constituents.to_dict("records")
    if limit:
        rows = rows[:limit]
    op = _build_opener()
    out = []
    t0 = time.time()

    for i, r in enumerate(rows, 1):
        code = r["stock_code"]
        html, tries = "", 0
        while True:
            tries += 1
            try:
                html = op.open(NIKKEI_COMPANY_URL.format(code=code), timeout=20) \
                         .read().decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"{code}: 取得エラー {e}")
                html = ""
            if len(html) >= _STUB_MAX:
                y, as_of, status = parse_nikkei_yield(html)
                break
            if tries >= _STUB_MAX_TRIES:
                y, as_of, status = None, None, f"stub_len{len(html)}"
                break
            logger.warning(f"{code}: スタブ応答 ({len(html)}B) → {_STUB_RETRY_WAIT}秒待機 (試行{tries})")
            time.sleep(_STUB_RETRY_WAIT)

        out.append({
            "date": pd.Timestamp(fetch_date), "stock_code": code,
            "stock_name": r.get("stock_name", ""), "nikkei_yield": y,
            "as_of": as_of, "status": status,
        })
        if i % 20 == 0 or i == len(rows):
            ok = sum(1 for o in out if o["nikkei_yield"] is not None)
            logger.info(f"{i}/{len(rows)} ok={ok} elapsed={time.time()-t0:.0f}s last={code} y={y} ({status})")

        time.sleep(_SLEEP_PER_CODE)
        if i % _BATCH_SIZE == 0 and i < len(rows):
            time.sleep(_BATCH_COOLDOWN)

    return pd.DataFrame(out)
