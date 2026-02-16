"""
先物ポジション分析ビュー - 原資産別 × ETF別棒グラフ + 指数二軸
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st

from config import FUTURES_MULTIPLIERS, TOPIX_ETF_CODES, NIKKEI225_ETF_CODES


def _format_yen(value: float) -> str:
    if pd.isna(value):
        return "---"
    abs_val = abs(value)
    if abs_val >= 1e12:
        return f"{value/1e12:.2f}兆円"
    elif abs_val >= 1e8:
        return f"{value/1e8:,.0f}億円"
    else:
        return f"{value:,.0f}円"


# ETF別色パレット
_ETF_COLORS = px.colors.qualitative.Set2 + px.colors.qualitative.Pastel + px.colors.qualitative.Set3

# 原資産グループ定義
# futures_type → 原資産グループ名
_UNDERLYING_GROUP = {
    "TOPIX": "TOPIX先物",
    "MINI_TOPIX": "TOPIX先物",
    "NK225": "日経225先物",
    "NK225_MINI": "日経225先物",
    "JPX400": "JPX日経400先物",
    "TSEREIT": "東証REIT先物",
    "JGB10Y": "10年国債先物",
    "TOPIX_BANKS": "TOPIX Banks先物",
    "TOPIX_CORE30": "Core30先物",
    "TSE_GROWTH": "グロース250先物",
    "JPX_PRIME150": "Prime150先物",
    "NK225_OPTION_CALL": "日経225先物",
    "NK225_OPTION_PUT": "日経225先物",
}

# 原資産グループ → 対応する指数カラム名（index_dfに含まれるもの）
_UNDERLYING_INDEX_MAP = {
    "TOPIX先物": "TOPIX",
    "日経225先物": "日経平均",
}

# 主要先物（選択肢に表示する順序）
_MAIN_UNDERLYINGS = [
    "TOPIX先物",
    "日経225先物",
    "JPX日経400先物",
    "東証REIT先物",
]

# 原資産グループ → 対象ETFコード（設定・交換で集計しているETFに限定）
# TOPIX/NK225 のみフィルタ、それ以外は全ETF表示
_UNDERLYING_ETF_FILTER: dict[str, list[str]] = {
    "TOPIX先物": TOPIX_ETF_CODES,
    "日経225先物": NIKKEI225_ETF_CODES,
}


def _add_underlying_group(futures_df: pd.DataFrame) -> pd.DataFrame:
    """futures_type を原資産グループに変換した列を追加する"""
    df = futures_df.copy()
    df["underlying"] = df["futures_type"].map(_UNDERLYING_GROUP).fillna("その他")
    return df


def render_futures_analysis(
    futures_df: pd.DataFrame,
    index_df: pd.DataFrame | None = None,
) -> None:
    """
    先物ポジション分析のメインレンダラー。
    原資産の選択UI + ETF別枚数棒グラフ + 指数二軸。
    """
    if futures_df.empty:
        st.info("先物データがありません")
        return

    df = _add_underlying_group(futures_df)

    # --- サマリーテーブル（原資産グループ単位） ---
    st.subheader("先物ポジション概要（原資産別）")
    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()

    summary = latest.groupby("underlying").agg(
        etf_count=("etf_code", "nunique"),
        total_quantity=("quantity", "sum"),
        total_market_value=("market_value", lambda x: x.abs().sum()),
    ).reset_index()

    summary["時価表示"] = summary["total_market_value"].apply(_format_yen)
    summary = summary.sort_values("total_market_value", ascending=False)

    st.dataframe(
        summary.rename(columns={
            "underlying": "原資産",
            "etf_count": "ETF数",
            "total_quantity": "合計枚数",
            "total_market_value": "合計時価",
        })[["原資産", "ETF数", "合計枚数", "時価表示"]],
        width="stretch",
        hide_index=True,
    )

    # --- 原資産選択UI ---
    st.markdown("---")
    available_underlyings = sorted(df["underlying"].unique())
    # 主要先物を先に並べる
    ordered = [u for u in _MAIN_UNDERLYINGS if u in available_underlyings]
    ordered += [u for u in available_underlyings if u not in ordered]

    selected_underlying = st.selectbox(
        "原資産を選択",
        options=ordered,
        index=0,
        key="futures_underlying_select",
    )

    # --- 選択した原資産のデータを抽出 ---
    underlying_df = df[df["underlying"] == selected_underlying].copy()

    # TOPIX/NK225 は設定・交換と同じETFに絞り込む
    etf_filter = _UNDERLYING_ETF_FILTER.get(selected_underlying)
    if etf_filter is not None:
        underlying_df = underlying_df[
            underlying_df["etf_code"].isin(etf_filter)
        ].copy()

    if underlying_df.empty:
        st.info(f"{selected_underlying} のデータがありません")
        return

    # --- ETF別の枚数棒グラフ（積み上げ） + 指数二軸 ---
    _render_etf_quantity_chart(underlying_df, selected_underlying, index_df)

    # --- ETF別の時価棒グラフ（積み上げ） ---
    _render_etf_market_value_chart(underlying_df, selected_underlying)

    # --- 限月別ポジション ---
    _render_contract_month_table(underlying_df, selected_underlying)


def _render_etf_quantity_chart(
    underlying_df: pd.DataFrame,
    underlying_name: str,
    index_df: pd.DataFrame | None = None,
) -> None:
    """ETF別の枚数推移（積み上げ棒グラフ） + 指数二軸"""
    st.subheader(f"{underlying_name} — ETF別建玉枚数")

    # ETFごとの日次合計枚数
    daily = underlying_df.groupby(["date", "etf_code"]).agg(
        total_quantity=("quantity", "sum"),
    ).reset_index()

    etf_codes = sorted(daily["etf_code"].unique())
    color_map = {
        code: _ETF_COLORS[i % len(_ETF_COLORS)]
        for i, code in enumerate(etf_codes)
    }

    # 指数データの準備
    idx_col = _UNDERLYING_INDEX_MAP.get(underlying_name)
    has_index = (
        idx_col is not None
        and index_df is not None
        and not index_df.empty
        and idx_col in index_df.columns
    )

    fig = make_subplots(
        specs=[[{"secondary_y": True}]],
    )

    for code in etf_codes:
        etf_data = daily[daily["etf_code"] == code]
        fig.add_trace(
            go.Bar(
                x=etf_data["date"],
                y=etf_data["total_quantity"],
                name=code,
                marker_color=color_map[code],
                hovertemplate=f"{code}<br>%{{x}}<br>%{{y:,.0f}}枚<extra></extra>",
            ),
            secondary_y=False,
        )

    # 指数の二軸折れ線
    if has_index:
        idx = index_df[["date", idx_col]].dropna()
        fig.add_trace(
            go.Scatter(
                x=idx["date"],
                y=idx[idx_col],
                name=idx_col,
                mode="lines",
                line=dict(width=2, color="rgba(0, 0, 0, 0.6)"),
            ),
            secondary_y=True,
        )
        fig.update_yaxes(title_text="指数", secondary_y=True)

    fig.update_layout(
        title=f"{underlying_name} ETF別建玉枚数推移",
        height=550,
        barmode="stack",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    fig.update_yaxes(title_text="枚数", secondary_y=False)
    fig.update_xaxes(title_text="日付")

    st.plotly_chart(fig, use_container_width=True)


def _render_etf_market_value_chart(
    underlying_df: pd.DataFrame,
    underlying_name: str,
) -> None:
    """ETF別の時価推移（積み上げ棒グラフ）"""
    st.subheader(f"{underlying_name} — ETF別時価残高")

    daily = underlying_df.groupby(["date", "etf_code"]).agg(
        total_market_value=("market_value", "sum"),
    ).reset_index()

    etf_codes = sorted(daily["etf_code"].unique())
    color_map = {
        code: _ETF_COLORS[i % len(_ETF_COLORS)]
        for i, code in enumerate(etf_codes)
    }

    fig = go.Figure()

    for code in etf_codes:
        etf_data = daily[daily["etf_code"] == code]
        fig.add_trace(
            go.Bar(
                x=etf_data["date"],
                y=etf_data["total_market_value"],
                name=code,
                marker_color=color_map[code],
                hovertemplate=f"{code}<br>%{{x}}<br>%{{customdata}}<extra></extra>",
                customdata=[_format_yen(v) for v in etf_data["total_market_value"]],
            )
        )

    fig.update_layout(
        title=f"{underlying_name} ETF別時価残高推移",
        height=500,
        barmode="stack",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
        yaxis_title="時価 (円)",
        xaxis_title="日付",
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_contract_month_table(
    underlying_df: pd.DataFrame,
    underlying_name: str,
) -> None:
    """限月別ポジション構成テーブル"""
    st.subheader(f"{underlying_name} — 限月別ポジション")

    latest_date = underlying_df["date"].max()
    latest = underlying_df[underlying_df["date"] == latest_date].copy()

    if latest["contract_month"].notna().any():
        month_summary = latest.groupby(["futures_type", "contract_month"]).agg(
            total_quantity=("quantity", "sum"),
            etf_count=("etf_code", "nunique"),
            total_market_value=("market_value", lambda x: x.abs().sum()),
        ).reset_index()

        month_summary["時価表示"] = month_summary["total_market_value"].apply(_format_yen)

        st.dataframe(
            month_summary.rename(columns={
                "futures_type": "先物種別",
                "contract_month": "限月",
                "total_quantity": "合計枚数",
                "etf_count": "ETF数",
                "total_market_value": "合計時価",
            })[["先物種別", "限月", "合計枚数", "ETF数", "時価表示"]],
            width="stretch",
            hide_index=True,
        )
    else:
        st.info("限月データがありません")
