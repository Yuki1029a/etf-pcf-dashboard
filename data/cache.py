"""
PCF CSVのローカルキャッシュ管理

ダウンロード済みCSVをローカルに保存し、再ダウンロードを防ぐ。
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from config import CACHE_DIR

logger = logging.getLogger(__name__)


def _get_cache_path(provider: str, etf_code: str, target_date: date) -> Path:
    """キャッシュファイルのパスを生成"""
    date_str = target_date.strftime("%Y%m%d")
    return CACHE_DIR / provider / f"{etf_code}_{date_str}.csv"


def get_cached_csv(provider: str, etf_code: str, target_date: date) -> str | None:
    """
    キャッシュからCSVテキストを取得する。

    Returns:
        CSVテキスト。キャッシュがなければ None。
    """
    path = _get_cache_path(provider, etf_code, target_date)
    if path.exists():
        text = path.read_text(encoding="utf-8", errors="replace")
        # 不正なキャッシュ（HTMLレスポンス等）を無効化
        if text.strip().startswith("<html") or text.strip().startswith("<!"):
            logger.debug(f"不正キャッシュを削除: {path}")
            path.unlink()
            return None
        logger.debug(f"キャッシュヒット: {path}")
        return text
    return None


def save_to_cache(
    provider: str, etf_code: str, target_date: date, csv_text: str
) -> Path:
    """
    CSVテキストをキャッシュに保存する。

    Returns:
        保存先パス
    """
    path = _get_cache_path(provider, etf_code, target_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(csv_text, encoding="utf-8")
    logger.debug(f"キャッシュ保存: {path}")
    return path


def list_cached_dates(provider: str, etf_code: str) -> list[date]:
    """指定ETFのキャッシュ済み日付一覧を返す"""
    provider_dir = CACHE_DIR / provider
    if not provider_dir.exists():
        return []

    dates = []
    for f in provider_dir.glob(f"{etf_code}_*.csv"):
        try:
            date_str = f.stem.split("_")[-1]
            d = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            dates.append(d)
        except (ValueError, IndexError):
            pass

    return sorted(dates)


def clear_old_cache(provider: str, keep_days: int = 30) -> int:
    """古いキャッシュファイルを削除する"""
    from datetime import timedelta
    cutoff = date.today() - timedelta(days=keep_days)
    provider_dir = CACHE_DIR / provider
    if not provider_dir.exists():
        return 0

    removed = 0
    for f in provider_dir.glob("*.csv"):
        try:
            date_str = f.stem.split("_")[-1]
            d = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
            if d < cutoff:
                f.unlink()
                removed += 1
        except (ValueError, IndexError):
            pass

    if removed:
        logger.info(f"{provider} キャッシュ: {removed} ファイル削除")
    return removed
