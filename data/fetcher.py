"""
PCF CSVダウンローダー

3つのプロバイダ（ICE Data Services, Solactive AG, S&P Global）から
PCF CSVを取得する。

注意: 営業日 7:50-23:55 JST のみダウンロード可能
"""
from __future__ import annotations

import logging
import time
import io
import zipfile
from datetime import date, datetime
from typing import Optional

import requests

from config import ICE_PCF_URL, SOLACTIVE_SINGLE_URL, SOLACTIVE_BULK_URL
from data.cache import get_cached_csv, save_to_cache

logger = logging.getLogger(__name__)

# リクエスト設定
REQUEST_TIMEOUT = 30  # 秒
REQUEST_INTERVAL = 0.5  # リクエスト間隔（秒）
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/csv,text/plain,*/*",
}


def _request_with_retry(url: str, retries: int = MAX_RETRIES) -> Optional[str]:
    """リトライ付きHTTPリクエスト"""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 200:
                # エンコーディング推定
                content = resp.content
                text = None
                for enc in ["utf-8", "shift_jis", "cp932", "latin-1"]:
                    try:
                        text = content.decode(enc)
                        break
                    except (UnicodeDecodeError, LookupError):
                        continue
                if text is None:
                    text = content.decode("utf-8", errors="replace")

                # HTMLレスポンスの検出（営業時間外等）
                if text.strip().startswith("<html") or text.strip().startswith("<!"):
                    logger.warning(f"HTMLレスポンス検出 (営業時間外?): {url}")
                    return None

                return text

            elif resp.status_code == 404:
                logger.debug(f"404 Not Found: {url}")
                return None

            elif resp.status_code == 403:
                logger.warning(f"403 Forbidden (営業時間外?): {url}")
                return None

            elif resp.status_code >= 500:
                logger.warning(
                    f"サーバーエラー {resp.status_code}: {url} "
                    f"(リトライ {attempt + 1}/{retries})"
                )
                time.sleep(2 ** attempt)
                continue

            else:
                logger.warning(f"HTTP {resp.status_code}: {url}")
                return None

        except requests.exceptions.Timeout:
            logger.warning(f"タイムアウト: {url} (リトライ {attempt + 1}/{retries})")
            time.sleep(2 ** attempt)
        except requests.exceptions.ConnectionError:
            logger.warning(f"接続エラー: {url} (リトライ {attempt + 1}/{retries})")
            time.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"予期しないエラー: {url}: {e}")
            return None

    logger.error(f"最大リトライ回数超過: {url}")
    return None


# ============================================================
# ICE Data Services
# ============================================================
def fetch_ice_pcf(etf_code: str, target_date: date = None) -> Optional[str]:
    """
    ICE Data ServicesからPCF CSVをダウンロードする。

    Args:
        etf_code: ETFコード (例: "1306")
        target_date: 対象日付 (Noneの場合は今日)

    Returns:
        CSVテキスト。失敗時はNone。
    """
    if target_date is None:
        target_date = date.today()

    # キャッシュ確認
    cached = get_cached_csv("ice", etf_code, target_date)
    if cached:
        return cached

    url = ICE_PCF_URL.format(code=etf_code)
    logger.info(f"ICE PCFダウンロード: {etf_code}")

    csv_text = _request_with_retry(url)
    if csv_text:
        save_to_cache("ice", etf_code, target_date, csv_text)
        time.sleep(REQUEST_INTERVAL)

    return csv_text


# ============================================================
# Solactive AG
# ============================================================
def fetch_solactive_pcf(etf_code: str, target_date: date = None) -> Optional[str]:
    """
    Solactive AGからPCF CSVをダウンロードする。

    Args:
        etf_code: ETFコード (例: "2640")
        target_date: 対象日付

    Returns:
        CSVテキスト。
    """
    if target_date is None:
        target_date = date.today()

    cached = get_cached_csv("solactive", etf_code, target_date)
    if cached:
        return cached

    url = SOLACTIVE_SINGLE_URL.format(code=etf_code)
    logger.info(f"Solactive PCFダウンロード: {etf_code}")

    csv_text = _request_with_retry(url)
    if csv_text:
        save_to_cache("solactive", etf_code, target_date, csv_text)
        time.sleep(REQUEST_INTERVAL)

    return csv_text


def fetch_solactive_bulk(target_date: date) -> dict[str, str]:
    """
    Solactiveの一括ZIPからPCF CSVを取得する。

    Args:
        target_date: 対象日付

    Returns:
        {ETFコード: CSVテキスト} の辞書
    """
    url = SOLACTIVE_BULK_URL.format(
        yyyy=target_date.strftime("%Y"),
        mm=target_date.strftime("%m"),
        dd=target_date.strftime("%d"),
    )
    logger.info(f"Solactive一括ダウンロード: {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        if resp.status_code != 200:
            logger.warning(f"Solactive一括ダウンロード失敗: HTTP {resp.status_code}")
            return {}

        results = {}
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                if name.endswith(".csv"):
                    # ファイル名からETFコードを抽出 (例: "2640.csv" -> "2640")
                    etf_code = name.replace(".csv", "").split("/")[-1]
                    csv_bytes = zf.read(name)

                    for enc in ["utf-8", "shift_jis", "cp932"]:
                        try:
                            csv_text = csv_bytes.decode(enc)
                            break
                        except UnicodeDecodeError:
                            continue
                    else:
                        csv_text = csv_bytes.decode("utf-8", errors="replace")

                    results[etf_code] = csv_text
                    save_to_cache("solactive", etf_code, target_date, csv_text)

        logger.info(f"Solactive一括ダウンロード完了: {len(results)} ファイル")
        return results

    except Exception as e:
        logger.error(f"Solactive一括ダウンロードエラー: {e}")
        return {}


# ============================================================
# 統合ダウンロード
# ============================================================
def fetch_pcf(
    etf_code: str,
    provider: str = "ice",
    target_date: date = None,
) -> Optional[str]:
    """
    プロバイダに応じたPCF CSVをダウンロードする。

    Args:
        etf_code: ETFコード
        provider: "ice", "solactive", "spglobal"
        target_date: 対象日付

    Returns:
        CSVテキスト。
    """
    if provider == "ice":
        return fetch_ice_pcf(etf_code, target_date)
    elif provider == "solactive":
        return fetch_solactive_pcf(etf_code, target_date)
    elif provider == "spglobal":
        logger.warning(f"S&P Globalプロバイダは未実装: {etf_code}")
        return None
    else:
        logger.error(f"不明なプロバイダ: {provider}")
        return None


def fetch_all_pcf(
    etf_codes: list[str],
    provider: str = "ice",
    target_date: date = None,
) -> dict[str, str]:
    """
    複数ETFのPCFを一括取得する。

    Returns:
        {ETFコード: CSVテキスト} の辞書
    """
    if target_date is None:
        target_date = date.today()

    results = {}
    failed = []

    for i, code in enumerate(etf_codes):
        csv_text = fetch_pcf(code, provider, target_date)
        if csv_text:
            results[code] = csv_text
        else:
            failed.append(code)

        if (i + 1) % 10 == 0:
            logger.info(f"  {i + 1}/{len(etf_codes)} 完了")

    logger.info(
        f"一括ダウンロード完了: 成功 {len(results)}, 失敗 {len(failed)}"
    )
    if failed:
        logger.warning(f"失敗ETF: {failed[:20]}{'...' if len(failed) > 20 else ''}")

    return results


def discover_etf_codes_from_jpx() -> list[str]:
    """
    JPXのPCFページからETFコード一覧をスクレイピングする。
    新規上場ETFの自動検出に使用。

    Returns:
        ETFコードのリスト
    """
    # JPXのインディカティブNAV・PCFページ
    url = "https://www.jpx.co.jp/equities/products/etfs/inav/index.html"
    logger.info(f"JPXからETFコード一覧を取得: {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning(f"JPXページ取得失敗: HTTP {resp.status_code}")
            return []

        import re
        # ICE PCFダウンロードリンクからETFコードを抽出
        # パターン: inav.ice.com/pcf-download/XXXX.csv
        ice_codes = re.findall(
            r"inav\.ice\.com/pcf-download/(\w+)\.csv",
            resp.text,
        )
        # Solactiveリンクからも抽出
        sol_codes = re.findall(
            r"solactive\.com/downloads/etfservices/tse-pcf/single/(\w+)\.csv",
            resp.text,
        )

        all_codes = list(set(ice_codes + sol_codes))
        logger.info(f"JPXから {len(all_codes)} ETFコードを検出")
        return sorted(all_codes)

    except Exception as e:
        logger.error(f"JPXスクレイピングエラー: {e}")
        return []
