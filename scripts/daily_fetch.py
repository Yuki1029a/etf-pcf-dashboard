"""
日次PCFデータ取得バッチスクリプト

JPXのPCFデータを自動ダウンロードし、ストアに追記する。
毎営業日 8:00-23:00 JST の間に実行すること。

Usage:
    python scripts/daily_fetch.py
    python scripts/daily_fetch.py --date 2026-02-12
    python scripts/daily_fetch.py --discover  # 新規ETF自動検出
"""
import sys
import argparse
import logging
from datetime import date, datetime
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import ETF_MASTER_PATH
from data.fetcher import fetch_ice_pcf, fetch_all_pcf, discover_etf_codes_from_jpx
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

    Args:
        target_date: 対象日付
        discover_new: 新規ETFを自動検出するか
    """
    logger.info(f"=== 日次PCFデータ取得 ({target_date}) ===")

    if not is_jpx_business_day(target_date):
        logger.warning(f"{target_date} は営業日ではありません")
        # 営業日でなくてもデータがある場合があるので続行

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

    if master_df.empty:
        logger.error("ETFマスタが空のため処理を中断します")
        return

    # ICEプロバイダのETFコード一覧
    etf_codes = master_df["code"].tolist()
    logger.info(f"対象ETF数: {len(etf_codes)}")

    # PCFダウンロード
    csv_results = fetch_all_pcf(etf_codes, provider="ice", target_date=target_date)
    logger.info(f"ダウンロード成功: {len(csv_results)} / {len(etf_codes)}")

    if not csv_results:
        logger.warning("ダウンロードされたデータがありません")
        return

    # パース
    records = []
    parse_errors = []

    for code, csv_text in csv_results.items():
        record = parse_pcf(csv_text, code, provider="ice")
        if record:
            records.append(record)
        else:
            parse_errors.append(code)

    logger.info(
        f"パース完了: 成功 {len(records)}, 失敗 {len(parse_errors)}"
    )

    if records:
        # DataFrame変換
        df = records_to_dataframe(records)

        # ストアに追記
        append_daily(df)
        logger.info(f"ストアに {len(df)} レコードを追記しました")
    else:
        logger.warning("パースされたレコードがありません")

    logger.info(f"=== 完了 ===")


def main():
    parser = argparse.ArgumentParser(description="日次PCFデータ取得")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="対象日付 (YYYY-MM-DD形式, デフォルト: 今日)",
    )
    parser.add_argument(
        "--discover",
        action="store_true",
        help="新規ETFを自動検出する",
    )
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = date.today()

    fetch_and_store(target_date, discover_new=args.discover)


if __name__ == "__main__":
    main()
