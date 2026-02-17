"""
日次PCFデータ取得バッチスクリプト

S&P Global (プライマリ) + ICE (フォールバック) の2プロバイダから
PCFデータを自動ダウンロードし、ストアに追記する。
毎営業日 8:00-23:00 JST の間に実行すること。

Usage:
    python scripts/daily_fetch.py
    python scripts/daily_fetch.py --date 2026-02-12
    python scripts/daily_fetch.py --range 2026-02-13 2026-02-17
    python scripts/daily_fetch.py --discover  # 新規ETF自動検出
"""
import sys
import argparse
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    ETF_MASTER_PATH, TOPIX_ETF_CODES, NIKKEI225_ETF_CODES,
)
from data.fetcher import (
    fetch_ice_pcf, fetch_all_pcf, fetch_spglobal_bulk,
    discover_etf_codes_from_jpx,
)
from data.parser_pcf import parse_pcf
from data.excel_importer import records_to_dataframe, masters_to_dataframe
from data.storage import (
    load_etf_master, save_etf_master, append_daily, update_etf_master,
)
from models import ETFMaster

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# 対象ETFコード（TOPIX + 日経225）
TARGET_CODES = set(TOPIX_ETF_CODES + NIKKEI225_ETF_CODES)


def is_jpx_business_day(d: date) -> bool:
    """JPXの営業日かどうかを判定"""
    # 土日チェック
    if d.weekday() >= 5:
        return False

    # 祝日チェック
    try:
        import jpholiday
        if jpholiday.is_holiday(d):
            return False
    except ImportError:
        logger.warning("jpholidayがインストールされていません。祝日チェックをスキップします。")

    return True


def fetch_and_store(target_date: date, discover_new: bool = False):
    """
    PCFデータを取得してストアに追記する。

    S&P Globalをプライマリプロバイダとして一括ZIP取得し、
    取得できなかった銘柄はICEでフォールバック取得する。

    Args:
        target_date: 対象日付
        discover_new: 新規ETFを自動検出するか
    """
    logger.info(f"=== 日次PCFデータ取得 ({target_date}) ===")

    if not is_jpx_business_day(target_date):
        logger.warning(f"{target_date} は営業日ではありません")

    # ETFマスタ読み込み
    master_df = load_etf_master()

    if master_df.empty:
        logger.warning("ETFマスタが空です。新規検出を実行します。")
        discover_new = True

    # 新規ETF検出
    if discover_new:
        new_codes = discover_etf_codes_from_jpx()
        if new_codes:
            existing_codes = set(master_df["code"]) if not master_df.empty else set()
            truly_new = [c for c in new_codes if c not in existing_codes]
            if truly_new:
                logger.info(f"新規ETF検出: {len(truly_new)} 件: {truly_new[:20]}")
                new_masters = []
                for code in truly_new:
                    new_masters.append({
                        "code": code,
                        "name": "",
                        "provider": "ice",
                        "category": "other",
                        "has_futures": False,
                    })
                import pandas as pd
                new_master_df = pd.DataFrame(new_masters)
                update_etf_master(new_master_df)
                master_df = load_etf_master()

    # ============================================================
    # Step 1: S&P Global 一括ZIP取得 (プライマリ)
    # ============================================================
    spg_results = fetch_spglobal_bulk(target_date)
    spg_target = {k: v for k, v in spg_results.items() if k in TARGET_CODES}
    logger.info(
        f"S&P Global: {len(spg_target)}/{len(TARGET_CODES)} 対象銘柄取得"
    )

    # ============================================================
    # Step 2: ICE フォールバック (S&P Globalで取れなかった銘柄)
    # ============================================================
    missing_codes = TARGET_CODES - set(spg_target.keys())
    ice_results = {}
    if missing_codes:
        logger.info(f"ICEフォールバック: {len(missing_codes)} 銘柄")
        ice_results = fetch_all_pcf(
            list(missing_codes), provider="ice", target_date=target_date
        )
        logger.info(f"ICE: {len(ice_results)}/{len(missing_codes)} 取得成功")

    # ============================================================
    # Step 3: パース
    # ============================================================
    records = []
    parse_errors = []

    # S&P Global分
    for code, csv_text in spg_target.items():
        record = parse_pcf(csv_text, code, provider="spglobal")
        if record:
            records.append(record)
        else:
            parse_errors.append(("spglobal", code))

    # ICE分
    for code, csv_text in ice_results.items():
        record = parse_pcf(csv_text, code, provider="ice")
        if record:
            records.append(record)
        else:
            parse_errors.append(("ice", code))

    logger.info(
        f"パース完了: 成功 {len(records)}, "
        f"失敗 {len(parse_errors)}"
    )
    if parse_errors:
        logger.warning(f"パース失敗: {parse_errors}")

    # ============================================================
    # Step 4: ストア追記
    # ============================================================
    if records:
        df = records_to_dataframe(records)
        append_daily(df)
        logger.info(f"ストアに {len(df)} レコードを追記しました")
    else:
        logger.warning("パースされたレコードがありません")

    # 取得できなかった銘柄
    all_fetched = set(spg_target.keys()) | set(ice_results.keys())
    still_missing = TARGET_CODES - all_fetched
    if still_missing:
        logger.warning(f"取得できなかった銘柄: {sorted(still_missing)}")

    logger.info(f"=== 完了 ({target_date}) ===")
    return len(records)


def main():
    parser = argparse.ArgumentParser(description="日次PCFデータ取得")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="対象日付 (YYYY-MM-DD形式, デフォルト: 今日)",
    )
    parser.add_argument(
        "--range",
        type=str,
        nargs=2,
        metavar=("FROM", "TO"),
        default=None,
        help="日付範囲 (YYYY-MM-DD YYYY-MM-DD)",
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="新規ETFを自動検出する",
    )
    args = parser.parse_args()

    if args.range:
        date_from = datetime.strptime(args.range[0], "%Y-%m-%d").date()
        date_to = datetime.strptime(args.range[1], "%Y-%m-%d").date()
        logger.info(f"日付範囲モード: {date_from} ~ {date_to}")

        current = date_from
        total = 0
        while current <= date_to:
            if is_jpx_business_day(current):
                count = fetch_and_store(current, discover_new=args.discover)
                total += count
            else:
                logger.info(f"{current} は非営業日のためスキップ")
            current += timedelta(days=1)

        logger.info(f"=== 全日程完了: 合計 {total} レコード ===")
    else:
        if args.date:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        else:
            target_date = date.today()

        fetch_and_store(target_date, discover_new=args.discover)


if __name__ == "__main__":
    main()
