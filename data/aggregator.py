"""
集計ロジックモジュール

設定・交換の規模計算、カテゴリ別集計、先物エクスポージャー計算。
"""
from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from config import (
    TOPIX_ETF_CODES, NIKKEI225_ETF_CODES, FUTURES_MULTIPLIERS,
    CATEGORY_CODE_MAP,
)

logger = logging.getLogger(__name__)


def compute_creation_redemption(df: pd.DataFrame) -> pd.DataFrame:
    """
    設定・交換の規模を計算する。

    ロジック:
        1. 各ETFの発行済口数 (shares_outstanding) の日次差分を計算
        2. 1口あたりNAV = NAV / shares_outstanding
        3. 設定・交換金額 = 口数増減 × 1口あたりNAV
        4. 口数増加 → 設定(creation)、口数減少 → 交換(redemption)

    Args:
        df: etf_timeseries DataFrame (etf_code, date, nav, shares_outstanding, ...)

    Returns:
        DataFrame with columns:
            etf_code, date, shares_outstanding, shares_change,
            nav_per_unit, flow_amount, flow_type
    """
    if df.empty:
        return pd.DataFrame()

    required_cols = ["etf_code", "date", "nav", "shares_outstanding"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"必須列がありません: {col}")

    # 日付でソート
    df = df.sort_values(["etf_code", "date"]).copy()

    # 口数の日次差分
    df["shares_change"] = df.groupby("etf_code")["shares_outstanding"].diff()

    # 1口あたりNAV（当日）
    df["nav_per_unit"] = df["nav"] / df["shares_outstanding"]
    df["nav_per_unit"] = df["nav_per_unit"].replace([float("inf"), float("-inf")], None)

    # 前日(t-1)の1口あたりNAV: 設定・交換は前日NAVで約定
    df["nav_per_unit_prev"] = df.groupby("etf_code")["nav_per_unit"].shift(1)

    # 設定・交換金額 = 口数増減(t) × 1口あたりNAV(t-1)
    df["flow_amount"] = df["shares_change"] * df["nav_per_unit_prev"]

    # フロータイプ
    df["flow_type"] = df["shares_change"].apply(
        lambda x: "creation" if pd.notna(x) and x > 0
        else ("redemption" if pd.notna(x) and x < 0 else None)
    )

    # 最初の日（差分が計算できない）を除外
    result = df[df["shares_change"].notna()].copy()

    cols = [
        "etf_code", "date", "shares_outstanding", "shares_change",
        "nav_per_unit", "flow_amount", "flow_type",
    ]
    return result[cols].reset_index(drop=True)


def _resolve_codes(
    category: str,
    cr_df: pd.DataFrame,
    master_df: Optional[pd.DataFrame] = None,
) -> list[str]:
    """カテゴリからETFコードリストを解決する"""
    if category in CATEGORY_CODE_MAP:
        return CATEGORY_CODE_MAP[category]
    elif category == "all":
        return cr_df["etf_code"].unique().tolist()
    elif master_df is not None and not master_df.empty:
        return master_df[master_df["category"] == category]["code"].tolist()
    return cr_df["etf_code"].unique().tolist()


def aggregate_by_category(
    cr_df: pd.DataFrame,
    category: str = "topix",
    master_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    カテゴリ別に日次設定・交換を集計する。

    Returns:
        日次集計 DataFrame:
            date, total_creation, total_redemption, net_flow, cumulative_flow
    """
    if cr_df.empty:
        return pd.DataFrame()

    codes = _resolve_codes(category, cr_df, master_df)
    filtered = cr_df[cr_df["etf_code"].isin(codes)].copy()

    if filtered.empty:
        return pd.DataFrame()

    daily = filtered.groupby("date").agg(
        total_creation=("flow_amount", lambda x: x[x > 0].sum()),
        total_redemption=("flow_amount", lambda x: x[x < 0].sum()),
        creation_count=("flow_type", lambda x: (x == "creation").sum()),
        redemption_count=("flow_type", lambda x: (x == "redemption").sum()),
    ).reset_index()

    daily["net_flow"] = daily["total_creation"] + daily["total_redemption"]
    daily["cumulative_flow"] = daily["net_flow"].cumsum()
    daily = daily.sort_values("date").reset_index(drop=True)

    return daily


def aggregate_etf_breakdown(
    cr_df: pd.DataFrame,
    category: str = "topix",
    master_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    カテゴリ内のETF別・日別のフローを返す（棒グラフのETF別色分け用）。

    Returns:
        DataFrame: etf_code, date, flow_amount, etf_label
    """
    if cr_df.empty:
        return pd.DataFrame()

    codes = _resolve_codes(category, cr_df, master_df)
    filtered = cr_df[cr_df["etf_code"].isin(codes)].copy()

    if filtered.empty:
        return pd.DataFrame()

    # ラベルはコードのみ
    filtered["etf_label"] = filtered["etf_code"]

    return filtered[["etf_code", "date", "flow_amount", "etf_label"]].copy()


def get_daily_ranking(
    cr_df: pd.DataFrame,
    target_date,
    category: str = "topix",
    master_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    指定日のETF別設定・交換ランキングを返す。
    """
    if cr_df.empty:
        return pd.DataFrame()

    codes = _resolve_codes(category, cr_df, master_df)
    day_df = cr_df[
        (cr_df["etf_code"].isin(codes))
        & (cr_df["date"] == pd.Timestamp(target_date))
    ].copy()

    if day_df.empty:
        return pd.DataFrame()

    if master_df is not None and not master_df.empty:
        name_map = master_df.set_index("code")["name"].to_dict()
        day_df["etf_label"] = day_df["etf_code"].map(
            lambda c: name_map.get(c, c)
        )
    else:
        day_df["etf_label"] = day_df["etf_code"]

    result = day_df[["etf_code", "etf_label", "flow_amount"]].copy()
    return result.sort_values("flow_amount", ascending=False).reset_index(drop=True)


def aggregate_nav_total(
    df: pd.DataFrame,
    category: str = "topix",
    master_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    カテゴリ別のNAV合計（資産残高）の時系列を返す。
    """
    if df.empty:
        return pd.DataFrame()

    codes = _resolve_codes(category, df, master_df)
    filtered = df[df["etf_code"].isin(codes)].copy()

    if filtered.empty:
        return pd.DataFrame()

    daily = filtered.groupby("date").agg(
        nav_total=("nav", "sum"),
        nav_mean=("nav", "mean"),
        nav_max=("nav", "max"),
        etf_count=("etf_code", "nunique"),
    ).reset_index()

    return daily.sort_values("date").reset_index(drop=True)


def aggregate_nav_etf_breakdown(
    df: pd.DataFrame,
    category: str = "topix",
    master_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    カテゴリ内のETF別・日別のNAV（資産残高）を返す。
    積み上げ棒グラフ用。

    Returns:
        DataFrame: etf_code, date, nav
    """
    if df.empty:
        return pd.DataFrame()

    codes = _resolve_codes(category, df, master_df)
    filtered = df[df["etf_code"].isin(codes)].copy()

    if filtered.empty:
        return pd.DataFrame()

    return filtered[["etf_code", "date", "nav"]].copy()


def aggregate_by_etf(
    cr_df: pd.DataFrame,
    etf_code: str,
) -> pd.DataFrame:
    """
    個別ETFの日次設定・交換を集計する。

    Returns:
        日次データ: date, shares_change, nav_per_unit, flow_amount, flow_type
    """
    if cr_df.empty:
        return pd.DataFrame()

    filtered = cr_df[cr_df["etf_code"] == etf_code].copy()
    if filtered.empty:
        return pd.DataFrame()

    filtered["cumulative_flow"] = filtered["flow_amount"].cumsum()
    return filtered.sort_values("date").reset_index(drop=True)


def compute_futures_exposure(df: pd.DataFrame) -> pd.DataFrame:
    """
    先物エクスポージャー（想定元本）を計算する。

    想定元本 = 建玉枚数 × 掛け目 × (先物残高 / 建玉枚数 / 掛け目)
             ≈ 先物残高 (PCFデータの先物残高が既に想定元本相当の場合)

    ただし、PCFの先物残高が時価評価の場合:
        想定元本 = 先物残高  (そのまま使用)
    """
    if df.empty:
        return pd.DataFrame()

    result_rows = []
    for _, row in df.iterrows():
        base = {
            "etf_code": row["etf_code"],
            "date": row["date"],
            "nav": row.get("nav"),
        }

        # 先物1
        if pd.notna(row.get("futures1_type")):
            result_rows.append({
                **base,
                "futures_type": row["futures1_type"],
                "contract_month": row.get("futures1_contract_month"),
                "quantity": row.get("futures1_quantity"),
                "market_value": row.get("futures1_market_value"),
                "multiplier": row.get("futures1_multiplier"),
            })

        # 先物2
        if pd.notna(row.get("futures2_type")):
            result_rows.append({
                **base,
                "futures_type": row["futures2_type"],
                "contract_month": row.get("futures2_contract_month"),
                "quantity": row.get("futures2_quantity"),
                "market_value": row.get("futures2_market_value"),
                "multiplier": row.get("futures2_multiplier"),
            })

    if not result_rows:
        return pd.DataFrame()

    futures_df = pd.DataFrame(result_rows)

    # 先物比率 (NAVに対する先物エクスポージャー)
    futures_df["futures_ratio"] = (
        futures_df["market_value"].abs() / futures_df["nav"]
    ).where(futures_df["nav"] > 0)

    return futures_df
