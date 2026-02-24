"""
時系列データの永続化モジュール

Parquet 形式でデータを保存・読込・追記する。
R2がプライマリストレージ、ローカルファイルはフォールバック。
R2未設定時（ローカル開発）は従来通りローカルファイルのみで動作。
"""
from __future__ import annotations

import io
import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from config import ETF_TIMESERIES_PATH, ETF_MASTER_PATH, STORE_DIR

logger = logging.getLogger(__name__)

# R2 キー定数
R2_TIMESERIES_KEY = "pcf/etf_timeseries.parquet"
R2_MASTER_KEY = "pcf/etf_master.csv"


def ensure_store_dir():
    """ストアディレクトリを作成"""
    STORE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# ETF 時系列データ
# ============================================================
def save_timeseries(df: pd.DataFrame, path: Path = ETF_TIMESERIES_PATH) -> None:
    """時系列DataFrameをParquetに保存（ローカル + R2）"""
    ensure_store_dir()

    # Parquetバイト列を生成
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    parquet_bytes = buf.getvalue()

    # ローカル保存
    path.write_bytes(parquet_bytes)
    logger.info(f"時系列データ保存 (local): {path} ({len(df)} 行)")

    # R2 保存
    from data.r2_storage import r2_put
    if r2_put(R2_TIMESERIES_KEY, parquet_bytes):
        logger.info(f"時系列データ保存 (R2): {R2_TIMESERIES_KEY}")


def load_timeseries(
    path: Path = ETF_TIMESERIES_PATH,
    etf_codes: Optional[list[str]] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> pd.DataFrame:
    """
    時系列データを読み込む（R2優先 → ローカルフォールバック）。

    Args:
        path: ローカルParquetファイルパス（フォールバック用）
        etf_codes: フィルタするETFコードリスト (Noneなら全件)
        date_from: 開始日 (Noneなら制限なし)
        date_to: 終了日 (Noneなら制限なし)

    Returns:
        フィルタ済みDataFrame
    """
    df = pd.DataFrame()

    # R2 から読み込み
    from data.r2_storage import r2_get
    content = r2_get(R2_TIMESERIES_KEY)
    if content is not None:
        df = pd.read_parquet(io.BytesIO(content), engine="pyarrow")
        logger.info(f"時系列データ読み込み (R2): {len(df)} 行")
    elif path.exists():
        # ローカルフォールバック
        df = pd.read_parquet(path, engine="pyarrow")
        logger.info(f"時系列データ読み込み (local): {len(df)} 行")
    else:
        logger.warning("時系列データが見つかりません (R2, local)")
        return pd.DataFrame()

    # フィルタ適用
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])

    if etf_codes is not None:
        df = df[df["etf_code"].isin(etf_codes)]

    if date_from is not None:
        df = df[df["date"] >= pd.Timestamp(date_from)]

    if date_to is not None:
        df = df[df["date"] <= pd.Timestamp(date_to)]

    return df.reset_index(drop=True)


def append_daily(new_df: pd.DataFrame, path: Path = ETF_TIMESERIES_PATH) -> None:
    """
    日次取得分を既存データに追記する（R2 + ローカル）。
    同一 (etf_code, date) の重複は新しいデータで上書き。
    """
    if new_df.empty:
        return

    # 既存データを読み込み（R2優先）
    existing = pd.DataFrame()
    from data.r2_storage import r2_get
    content = r2_get(R2_TIMESERIES_KEY)
    if content is not None:
        existing = pd.read_parquet(io.BytesIO(content), engine="pyarrow")
        logger.info(f"既存データ読み込み (R2): {len(existing)} 行")
    elif path.exists():
        existing = pd.read_parquet(path, engine="pyarrow")
        logger.info(f"既存データ読み込み (local): {len(existing)} 行")

    if not existing.empty:
        existing["date"] = pd.to_datetime(existing["date"])
        new_df["date"] = pd.to_datetime(new_df["date"])

        # 重複除去: 新データを優先
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["etf_code", "date"], keep="last"
        )
        combined = combined.sort_values(["etf_code", "date"]).reset_index(drop=True)
    else:
        combined = new_df

    save_timeseries(combined, path)
    logger.info(f"日次追記完了: +{len(new_df)} 行 → 合計 {len(combined)} 行")


# ============================================================
# ETF マスタ
# ============================================================
def save_etf_master(df: pd.DataFrame, path: Path = ETF_MASTER_PATH) -> None:
    """ETFマスタをCSVに保存（ローカル + R2）"""
    ensure_store_dir()

    csv_content = df.to_csv(index=False, encoding="utf-8-sig")
    path.write_text(csv_content, encoding="utf-8-sig")
    logger.info(f"ETFマスタ保存 (local): {path} ({len(df)} 件)")

    # R2 保存
    from data.r2_storage import r2_put
    if r2_put(R2_MASTER_KEY, csv_content.encode("utf-8-sig")):
        logger.info(f"ETFマスタ保存 (R2): {R2_MASTER_KEY}")


def load_etf_master(path: Path = ETF_MASTER_PATH) -> pd.DataFrame:
    """ETFマスタをCSVから読み込む（R2優先 → ローカルフォールバック）"""
    # R2 から読み込み
    from data.r2_storage import r2_get
    content = r2_get(R2_MASTER_KEY)
    if content is not None:
        logger.info("ETFマスタ読み込み (R2)")
        return pd.read_csv(io.BytesIO(content), encoding="utf-8-sig")

    # ローカルフォールバック
    if path.exists():
        logger.info("ETFマスタ読み込み (local)")
        return pd.read_csv(path, encoding="utf-8-sig")

    logger.warning("ETFマスタが見つかりません (R2, local)")
    return pd.DataFrame()


def update_etf_master(new_df: pd.DataFrame, path: Path = ETF_MASTER_PATH) -> None:
    """ETFマスタを更新 (code で重複除去、新データ優先)"""
    existing = load_etf_master(path)
    if not existing.empty:
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["code"], keep="last")
    else:
        combined = new_df
    save_etf_master(combined, path)
