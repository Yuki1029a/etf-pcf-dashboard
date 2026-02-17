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

from config import (
    ICE_PCF_URL, ICE_BULK_ZIP_URL, ICE_LIST_ZIPS_URL,
    SOLACTIVE_SINGLE_URL, SOLACTIVE_BULK_URL,
    SPGLOBAL_FILEDATES_URL, SPGLOBAL_FILE_URL, SPGLOBAL_HEADERS,
)
from data.cache import get_cached_csv, save_to_cache

logger = logging.getLogger(__name__)

# リクエスト設定
REQUEST_TIMEOUT = 30  # 秒
REQUEST_INTERVAL = 0.5  # リクエスト間隔（秒）
MAX_RETRIES = 3

HEADERS = {
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
def fetch_ice_filedates() -> list[str]:
    """
    ICEから利用可能なPCF ZIP日付一覧を取得する。

    Returns:
        日付文字列のリスト (例: ["20260217", "20260216", ...])
    """
    try:
        resp = requests.get(ICE_LIST_ZIPS_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            import re
            dates = re.findall(r"all_pcf_(\d{8})\.zip", resp.text)
            logger.info(f"ICE 利用可能日付: {len(dates)} 件")
            return dates
        else:
            logger.warning(f"ICE listOfZips取得失敗: HTTP {resp.status_code}")
            return []
    except Exception as e:
        logger.error(f"ICE listOfZipsエラー: {e}")
        return []


def fetch_ice_bulk(target_date: date) -> dict[str, str]:
    """
    ICEのPCF一括ZIPからCSVを取得する。

    Args:
        target_date: 対象日付

    Returns:
        {ETFコード: CSVテキスト} の辞書
    """
    date_str = target_date.strftime("%Y%m%d")
    url = ICE_BULK_ZIP_URL.format(date=date_str)
    logger.info(f"ICE一括ダウンロード: all_pcf_{date_str}.zip")

    try:
        resp = requests.get(
            url, headers={**HEADERS, "Cache-Control": "no-cache, no-store"},
            timeout=60,
        )
        if resp.status_code != 200:
            logger.warning(f"ICE一括ダウンロード失敗: HTTP {resp.status_code}")
            return {}

        results = {}
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                if not name.endswith(".csv"):
                    continue

                # ファイル名: "1306tsepcf_Feb122026.csv" or "1321osepcf_Feb162026.csv" -> ETFコード
                import re
                m = re.match(r"^(\w+?)(?:tsepcf|osepcf)_", name)
                if m:
                    etf_code = m.group(1)
                else:
                    etf_code = name.replace(".csv", "").split("_")[0]

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
                save_to_cache("ice", etf_code, target_date, csv_text)

        logger.info(f"ICE一括ダウンロード完了: {len(results)} ファイル")
        return results

    except Exception as e:
        logger.error(f"ICE一括ダウンロードエラー: {e}")
        return {}


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


def fetch_spglobal_filedates() -> list[str]:
    """
    S&P Globalから利用可能なPCF日付一覧を取得する。

    Returns:
        日付文字列のリスト (例: ["2026/02/17", "2026/02/16", ...])
    """
    try:
        resp = requests.get(
            SPGLOBAL_FILEDATES_URL,
            headers=SPGLOBAL_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 200:
            dates = resp.json()
            logger.info(f"S&P Global 利用可能日付: {len(dates)} 件")
            return dates
        else:
            logger.warning(f"S&P Global filedates取得失敗: HTTP {resp.status_code}")
            return []
    except Exception as e:
        logger.error(f"S&P Global filedatesエラー: {e}")
        return []


def fetch_spglobal_bulk(target_date: date) -> dict[str, str]:
    """
    S&P GlobalのPCF一括ZIPからCSVを取得する。

    Args:
        target_date: 対象日付

    Returns:
        {ETFコード: CSVテキスト} の辞書
    """
    date_str = target_date.strftime("%Y%m%d")
    zip_filename = f"all_pcf_{date_str}.zip"
    url = f"{SPGLOBAL_FILE_URL}?filename={zip_filename}"
    logger.info(f"S&P Global一括ダウンロード: {zip_filename}")

    try:
        resp = requests.get(url, headers=SPGLOBAL_HEADERS, timeout=60)
        if resp.status_code != 200:
            logger.warning(
                f"S&P Global一括ダウンロード失敗: HTTP {resp.status_code}"
            )
            return {}

        # ZIP解凍
        results = {}
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in zf.namelist():
                if not name.endswith(".csv"):
                    continue

                # ファイル名からETFコードを抽出 (例: "1306_20260217.csv" -> "1306")
                basename = name.split("/")[-1].replace(".csv", "")
                etf_code = basename.split("_")[0]

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
                save_to_cache("spglobal", etf_code, target_date, csv_text)

        logger.info(f"S&P Global一括ダウンロード完了: {len(results)} ファイル")
        return results

    except Exception as e:
        logger.error(f"S&P Global一括ダウンロードエラー: {e}")
        return {}


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
        # 個別取得はキャッシュからのみ (一括DLは fetch_spglobal_bulk を使用)
        cached = get_cached_csv("spglobal", etf_code, target_date)
        if cached:
            return cached
        logger.debug(f"S&P Globalキャッシュなし: {etf_code} (一括DLを使用してください)")
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
