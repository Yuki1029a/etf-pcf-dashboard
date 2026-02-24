"""
ワンタイム移行スクリプト: ローカルデータを Cloudflare R2 にアップロード

既存の etf_timeseries.parquet に market_value_type カラムを付与し、
R2バケット (pcf-data) にアップロードする。

Usage:
    # .streamlit/secrets.toml または環境変数に R2 クレデンシャルを設定してから:
    python scripts/migrate_to_r2.py

    # 環境変数で直接指定する場合:
    R2_ACCOUNT_ID=xxx R2_ACCESS_KEY_ID=xxx R2_SECRET_ACCESS_KEY=xxx R2_BUCKET_NAME=pcf-data python scripts/migrate_to_r2.py
"""
import sys
import io
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

from config import ETF_TIMESERIES_PATH, ETF_MASTER_PATH
from data.r2_storage import r2_put, r2_available

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Excel→PCF CSV 切り替え日
# Excel由来データは 2026-02-12 まで、PCF CSVは 2026-02-13 から
EXCEL_CUTOFF_DATE = pd.Timestamp("2026-02-12")


def migrate_timeseries():
    """etf_timeseries.parquet に market_value_type を付与して R2 にアップロード"""
    if not ETF_TIMESERIES_PATH.exists():
        logger.error(f"時系列データが見つかりません: {ETF_TIMESERIES_PATH}")
        return False

    logger.info(f"時系列データ読み込み: {ETF_TIMESERIES_PATH}")
    df = pd.read_parquet(ETF_TIMESERIES_PATH, engine="pyarrow")
    df["date"] = pd.to_datetime(df["date"])
    logger.info(f"  行数: {len(df)}")
    logger.info(f"  日付範囲: {df['date'].min()} ~ {df['date'].max()}")
    logger.info(f"  ETF数: {df['etf_code'].nunique()}")

    # market_value_type カラムを追加
    if "market_value_type" not in df.columns:
        logger.info("market_value_type カラムを追加中...")
        df["market_value_type"] = df["date"].apply(
            lambda d: "notional" if d <= EXCEL_CUTOFF_DATE else "mtm"
        )
        notional_count = (df["market_value_type"] == "notional").sum()
        mtm_count = (df["market_value_type"] == "mtm").sum()
        logger.info(f"  notional (Excel): {notional_count} 行")
        logger.info(f"  mtm (PCF CSV): {mtm_count} 行")

        # ローカルにも保存（市場value_type付き）
        df.to_parquet(ETF_TIMESERIES_PATH, index=False, engine="pyarrow")
        logger.info(f"ローカル更新完了: {ETF_TIMESERIES_PATH}")
    else:
        logger.info("market_value_type カラムは既に存在します")

    # R2 にアップロード
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    parquet_bytes = buf.getvalue()

    logger.info(f"R2 にアップロード中 (pcf/etf_timeseries.parquet, {len(parquet_bytes):,} bytes)...")
    if r2_put("pcf/etf_timeseries.parquet", parquet_bytes):
        logger.info("R2 アップロード成功: pcf/etf_timeseries.parquet")
        return True
    else:
        logger.error("R2 アップロード失敗: pcf/etf_timeseries.parquet")
        return False


def migrate_master():
    """etf_master.csv を R2 にアップロード"""
    if not ETF_MASTER_PATH.exists():
        logger.error(f"ETFマスタが見つかりません: {ETF_MASTER_PATH}")
        return False

    logger.info(f"ETFマスタ読み込み: {ETF_MASTER_PATH}")
    content = ETF_MASTER_PATH.read_bytes()
    logger.info(f"  サイズ: {len(content):,} bytes")

    logger.info("R2 にアップロード中 (pcf/etf_master.csv)...")
    if r2_put("pcf/etf_master.csv", content):
        logger.info("R2 アップロード成功: pcf/etf_master.csv")
        return True
    else:
        logger.error("R2 アップロード失敗: pcf/etf_master.csv")
        return False


def main():
    logger.info("=== R2 移行スクリプト開始 ===")

    if not r2_available():
        logger.error(
            "R2 が利用できません。以下のいずれかを設定してください:\n"
            "  1. .streamlit/secrets.toml に [r2] セクション\n"
            "  2. 環境変数: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME"
        )
        sys.exit(1)

    success = True

    # 時系列データ
    if not migrate_timeseries():
        success = False

    # ETFマスタ
    if not migrate_master():
        success = False

    if success:
        logger.info("=== 移行完了 ===")
    else:
        logger.error("=== 移行に一部失敗がありました ===")
        sys.exit(1)


if __name__ == "__main__":
    main()
