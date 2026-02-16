"""
資産残高（NAV合計）時系列ビュー - ETF別積み上げ棒グラフ
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st


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


# 色パレット
_ETF_COLORS = px.colors.qualitative.Set3 + px.colors.qualitative.Pastel


def render_nav_timeseries(
    nav_df: pd.DataFrame,
    title: str = "資産残高（NAV合計）",
    index_df: pd.DataFrame | None = None,
    etf_breakdown: pd.DataFrame | None = None,
) -> None:
    """
    資産残高をETF別積み上げ棒グラフで表示。
    index_df が渡された場合、二軸で指数を重ねる。
    """
    if nav_df.empty:
        st.warning("データがありません")
        return

    has_index = index_df is not None and not index_df.empty

    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"secondary_y": True}]] if has_index else None,
    )

    if etf_breakdown is not None and not etf_breakdown.empty:
        # ETF別積み上げ棒グラフ
        etf_codes = sorted(etf_breakdown["etf_code"].unique())
        color_map = {
            code: _ETF_COLORS[i % len(_ETF_COLORS)]
            for i, code in enumerate(etf_codes)
        }

        for code in etf_codes:
            etf_data = etf_breakdown[etf_breakdown["etf_code"] == code]
            daily_etf = etf_data.groupby("date")["nav"].sum().reset_index()

            fig.add_trace(
                go.Bar(
                    x=daily_etf["date"],
                    y=daily_etf["nav"],
                    name=code,
                    marker_color=color_map[code],
                    hovertemplate=f"{code}<br>%{{x}}<br>%{{customdata}}<extra></extra>",
                    customdata=[_format_yen(v) for v in daily_etf["nav"]],
                ),
                secondary_y=False,
            )
    else:
        # フォールバック: 合計棒グラフ
        fig.add_trace(
            go.Bar(
                x=nav_df["date"],
                y=nav_df["nav_total"],
                name="NAV合計",
                marker_color="rgba(55, 128, 235, 0.7)",
                hovertemplate="%{x}<br>合計: %{customdata}<extra></extra>",
                customdata=[_format_yen(v) for v in nav_df["nav_total"]],
            ),
            secondary_y=False,
        )

    # 指数の二軸グラフ
    if has_index:
        for col in index_df.columns:
            if col == "date":
                continue
            fig.add_trace(
                go.Scatter(
                    x=index_df["date"],
                    y=index_df[col],
                    name=col,
                    mode="lines",
                    line=dict(width=2),
                    opacity=0.8,
                ),
                secondary_y=True,
            )
        fig.update_yaxes(title_text="指数", secondary_y=True)

    fig.update_layout(
        title=title,
        height=500,
        barmode="stack",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )
    fig.update_yaxes(title_text="NAV (円)", secondary_y=False)

    st.plotly_chart(fig, width="stretch")

    # サマリーメトリクス
    col1, col2, col3 = st.columns(3)
    with col1:
        latest = nav_df.iloc[-1]["nav_total"] if len(nav_df) > 0 else 0
        st.metric("最新NAV合計", _format_yen(latest))
    with col2:
        avg = nav_df["nav_total"].mean()
        st.metric("期間平均NAV合計", _format_yen(avg))
    with col3:
        etf_count = int(nav_df.iloc[-1]["etf_count"]) if len(nav_df) > 0 else 0
        st.metric("ETF数", etf_count)
