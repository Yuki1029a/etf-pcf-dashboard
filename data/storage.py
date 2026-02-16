"""
時系列データの永続化モジュール

Parquet 形式でデータを保存・読込・追記する。
将来のクラウド移行はこのモジュールのバックエンドを差し替えるだけで対応可能。
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from config import ETF_TIMESERIES_PATH, ETF_MASTER_PATH, STORE_DIR

logger = logging.getLogger(__name__)


def ensure_store_dir():
    """ストアディレクトリを作成"""
    STORE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# ETF 時系列データ
# ============================================================
def save_timeseries(df: pd.DataFrame, path: Path = ETF_TIMESERIES_PATH) -> None:
    """時系列DataFrameをParquetに保存"""
    ensure_store_dir()
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(f"時系列データ保存: {path} ({len(df)} 行)")


def load_timeseries(
    path: Path = ETF_TIMESERIES_PATH,
    etf_codes: Optional[list[str]] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> pd.DataFrame:
    """
    Parquetから時系列データを読み込む。

    Args:
        path: Parquetファイルパス
        etf_codes: フィルタするETFコードリスト (Noneなら全件)
        date_from: 開始日 (Noneなら制限なし)
        date_to: 終了日 (Noneなら制限なし)

    Returns:
        フィルタ済みDataFrame
    """
    if not path.exists():
        logger.warning(f"ファイルが見つかりません: {path}")
        return pd.DataFrame()

    df = pd.read_parquet(path, engine="pyarrow")

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
    日次取得分を既存Parquetに追記する。
    同一 (etf_code, date) の重複は新しいデータで上書き。
    """
    if new_df.empty:
        return

    if path.exists():
        existing = pd.read_parquet(path, engine="pyarrow")
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
    """ETFマスタをCSVに保存"""
    ensure_store_dir()
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info(f"ETFマスタ保存: {path} ({len(df)} 件)")


def load_etf_master(path: Path = ETF_MASTER_PATH) -> pd.DataFrame:
    """ETFマスタをCSVから読み込む"""
    if not path.exists():
        logger.warning(f"ファイルが見つかりません: {path}")
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def update_etf_master(new_df: pd.DataFrame, path: Path = ETF_MASTER_PATH) -> None:
    """ETFマスタを更新 (code で重複除去、新データ優先)"""
    if path.exists():
        existing = load_etf_master(path)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["code"], keep="last")
    else:
        combined = new_df
    save_etf_master(combined, path)
