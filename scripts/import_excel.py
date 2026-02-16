"""
既存 pcf_getter (2).xlsm から Parquet ストアへのデータ移行スクリプト

Usage:
    python scripts/import_excel.py
"""
import sys
import logging
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import EXCEL_PATH
from data.excel_importer import import_excel, records_to_dataframe, masters_to_dataframe
from data.storage import save_timeseries, save_etf_master

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    logger.info(f"=== Excel データ移行開始 ===")
    logger.info(f"ソースファイル: {EXCEL_PATH}")

    if not EXCEL_PATH.exists():
        logger.error(f"ファイルが見つかりません: {EXCEL_PATH}")
        sys.exit(1)

    # 1. Excelからデータ読み込み
    records, masters = import_excel(EXCEL_PATH)
    logger.info(f"読み込み完了: {len(masters)} ETF, {len(records)} レコード")

    # 2. DataFrame に変換
    df_ts = records_to_dataframe(records)
    df_master = masters_to_dataframe(masters)

    # 3. 統計表示
    logger.info(f"\n=== データ統計 ===")
    logger.info(f"ETF数: {df_master['code'].nunique()}")
    logger.info(f"カテゴリ別:")
    for cat, count in df_master["category"].value_counts().items():
        logger.info(f"  {cat}: {count}")
    logger.info(f"先物あり: {df_master['has_futures'].sum()}")
    logger.info(f"日付範囲: {df_ts['date'].min()} ～ {df_ts['date'].max()}")
    logger.info(f"総レコード数: {len(df_ts)}")

    # 先物パターンの確認
    futures_types = df_ts[df_ts["futures1_type"].notna()]["futures1_type"].value_counts()
    if not futures_types.empty:
        logger.info(f"\n先物種別 (先物1):")
        for ft, count in futures_types.items():
            logger.info(f"  {ft}: {count}")

    futures2_types = df_ts[df_ts["futures2_type"].notna()]["futures2_type"].value_counts()
    if not futures2_types.empty:
        logger.info(f"\n先物種別 (先物2):")
        for ft, count in futures2_types.items():
            logger.info(f"  {ft}: {count}")

    # UNKNOWN先物の詳細
    unknown = df_ts[df_ts["futures1_type"] == "UNKNOWN"]["futures1_raw_name"].unique()
    if len(unknown) > 0:
        logger.warning(f"\n未分類の先物銘柄名 (先物1):")
        for name in unknown:
            logger.warning(f"  '{name}'")

    unknown2 = df_ts[df_ts["futures2_type"] == "UNKNOWN"]["futures2_raw_name"].unique()
    if len(unknown2) > 0:
        logger.warning(f"\n未分類の先物銘柄名 (先物2):")
        for name in unknown2:
            logger.warning(f"  '{name}'")

    # 4. Parquet に保存
    save_timeseries(df_ts)
    save_etf_master(df_master)

    logger.info(f"\n=== 移行完了 ===")


if __name__ == "__main__":
    main()
