"""
日経225採用銘柄 予想配当利回り（日経予想）日次取得バッチ

Usage:
    python scripts/fetch_nikkei_yield.py              # 当日(JST)
    python scripts/fetch_nikkei_yield.py --date 2026-10-26
    python scripts/fetch_nikkei_yield.py --limit 5    # 動作確認用
"""
import sys
import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.nikkei_yield import get_nikkei225_constituents, fetch_nikkei_yields
from data.storage import load_holdings, append_nikkei_yield

JST = timezone(timedelta(hours=9))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="日経225 予想配当利回り取得")
    parser.add_argument("--date", type=str, default=None, help="取得日 (YYYY-MM-DD, デフォルト: JST当日)")
    parser.add_argument("--limit", type=int, default=None, help="先頭N銘柄のみ（動作確認用）")
    args = parser.parse_args()

    target = (datetime.strptime(args.date, "%Y-%m-%d").date() if args.date
              else datetime.now(JST).date())
    logger.info(f"=== 日経予想利回り取得 ({target}) ===")

    cons = get_nikkei225_constituents(load_holdings())
    if cons.empty:
        logger.error("採用銘柄が取得できません（1321 の保有データなし）")
        sys.exit(1)

    df = fetch_nikkei_yields(cons, target, limit=args.limit)
    ok = df["nikkei_yield"].notna().sum()
    logger.info(f"取得完了: ok={ok} / {len(df)}")
    bad = df[df["status"] != "ok"]
    if not bad.empty:
        logger.warning(f"未取得/無配: {bad[['stock_code', 'stock_name', 'status']].to_dict('records')}")

    append_nikkei_yield(df)
    logger.info(f"=== 完了 ({target}) ===")


if __name__ == "__main__":
    main()
