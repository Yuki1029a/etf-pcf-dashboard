"""
設定・交換グラフ (Plotly) + 日次ランキング
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st


def _format_yen(value: float) -> str:
    """金額を読みやすい形式に変換"""
    if pd.isna(value):
        return "---"
    abs_val = abs(value)
    if abs_val >= 1e12:
        return f"{value/1e12:.1f}兆円"
    elif abs_val >= 1e8:
        return f"{value/1e8:.0f}億円"
    elif abs_val >= 1e4:
        return f"{value/1e4:.0f}万円"
    else:
        return f"{value:.0f}円"


def _format_yen_table(value: float) -> str:
    """テーブル用: 億円で丸めて表示"""
    if pd.isna(value):
        return "---"
    return f"{value/1e8:,.0f}億円"


# Plotlyの色パレット（ETF別の色分け）
_ETF_COLORS = px.colors.qualitative.Set3 + px.colors.qualitative.Pastel


def render_creation_redemption_chart(
    daily_df: pd.DataFrame,
    title: str = "設定・交換の推移",
    etf_breakdown: pd.DataFrame | None = None,
    index_df: pd.DataFrame | None = None,
) -> None:
    """
    設定・交換の日次棒グラフ + 累積フロー + 指数二軸。
    etf_breakdown が渡された場合、ETF別の積み上げ棒グラフを表示。
    index_df が渡された場合、二軸で指数終値を重ねる。
    """
    if daily_df.empty:
        st.warning("データがありません")
        return

    has_index = index_df is not None and not index_df.empty

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.6, 0.4],
        subplot_titles=("日次 設定・交換金額", "累積ネットフロー"),
        specs=[
            [{"secondary_y": True}],
            [{"secondary_y": False}],
        ],
    )

    if etf_breakdown is not None and not etf_breakdown.empty:
        etf_labels = etf_breakdown["etf_label"].unique()
        color_map = {
            label: _ETF_COLORS[i % len(_ETF_COLORS)]
            for i, label in enumerate(sorted(etf_labels))
        }

        for etf_label in sorted(etf_labels):
            etf_data = etf_breakdown[etf_breakdown["etf_label"] == etf_label]
            daily_etf = etf_data.groupby("date")["flow_amount"].sum().reset_index()

            # 設定(正)
            creation = daily_etf.copy()
            creation["flow_amount"] = creation["flow_amount"].clip(lower=0)
            fig.add_trace(
                go.Bar(
                    x=creation["date"],
                    y=creation["flow_amount"],
                    name=etf_label,
                    marker_color=color_map[etf_label],
                    legendgroup=etf_label,
                    hovertemplate=f"{etf_label}<br>%{{x}}<br>%{{customdata}}<extra></extra>",
                    customdata=[_format_yen(v) for v in creation["flow_amount"]],
                ),
                row=1, col=1, secondary_y=False,
            )

            # 交換(負)
            redemption = daily_etf.copy()
            redemption["flow_amount"] = redemption["flow_amount"].clip(upper=0)
            has_redemption = (redemption["flow_amount"] < 0).any()
            if has_redemption:
                fig.add_trace(
                    go.Bar(
                        x=redemption["date"],
                        y=redemption["flow_amount"],
                        name=etf_label,
                        marker_color=color_map[etf_label],
                        legendgroup=etf_label,
                        showlegend=False,
                        hovertemplate=f"{etf_label}<br>%{{x}}<br>%{{customdata}}<extra></extra>",
                        customdata=[_format_yen(v) for v in redemption["flow_amount"]],
                    ),
                    row=1, col=1, secondary_y=False,
                )
    else:
        fig.add_trace(
            go.Bar(
                x=daily_df["date"],
                y=daily_df["total_creation"],
                name="設定",
                marker_color="rgba(55, 128, 235, 0.7)",
                hovertemplate="%{x}<br>設定: %{customdata}<extra></extra>",
                customdata=[_format_yen(v) for v in daily_df["total_creation"]],
            ),
            row=1, col=1, secondary_y=False,
        )
        fig.add_trace(
            go.Bar(
                x=daily_df["date"],
                y=daily_df["total_redemption"],
                name="交換",
                marker_color="rgba(235, 55, 55, 0.7)",
                hovertemplate="%{x}<br>交換: %{customdata}<extra></extra>",
                customdata=[_format_yen(v) for v in daily_df["total_redemption"]],
            ),
            row=1, col=1, secondary_y=False,
        )

    # 指数の二軸折れ線（上段に重ねる）
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
                    line=dict(width=2, color="rgba(0, 0, 0, 0.6)"),
                ),
                row=1, col=1, secondary_y=True,
            )
        fig.update_yaxes(title_text="指数", secondary_y=True, row=1, col=1)

    # 下段: 累積フロー
    fig.add_trace(
        go.Scatter(
            x=daily_df["date"],
            y=daily_df["cumulative_flow"],
            name="累積フロー",
            mode="lines",
            fill="tozeroy",
            line=dict(color="rgba(55, 180, 55, 0.8)", width=2),
            fillcolor="rgba(55, 180, 55, 0.15)",
            hovertemplate="%{x}<br>累積: %{customdata}<extra></extra>",
            customdata=[_format_yen(v) for v in daily_df["cumulative_flow"]],
        ),
        row=2, col=1,
    )

    fig.update_layout(
        title=title,
        height=700,
        barmode="relative",
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified",
    )

    fig.update_yaxes(title_text="金額 (円)", secondary_y=False, row=1, col=1)
    fig.update_yaxes(title_text="累積 (円)", row=2, col=1)

    st.plotly_chart(fig, width="stretch")


def render_daily_ranking(
    ranking_df: pd.DataFrame,
    target_date,
) -> None:
    """
    指定日のETF別設定・交換ランキングテーブルを表示する。
    """
    if ranking_df.empty:
        st.info("選択日のデータがありません")
        return

    display = ranking_df.copy()
    display["金額"] = display["flow_amount"].apply(_format_yen_table)
    display = display.rename(columns={
        "etf_code": "コード",
        "etf_label": "名称",
    })

    st.dataframe(
        display[["コード", "名称", "金額"]],
        width="stretch",
        hide_index=True,
    )


def render_etf_detail_chart(
    etf_df: pd.DataFrame,
    etf_code: str,
) -> None:
    """
    個別ETFの設定・交換チャートを描画する。
    """
    if etf_df.empty:
        st.warning(f"{etf_code} のデータがありません")
        return

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.5, 0.5],
        subplot_titles=(
            f"{etf_code} 設定・交換金額",
            f"{etf_code} 口数推移",
        ),
    )

    colors = [
        "rgba(55, 128, 235, 0.7)" if v >= 0 else "rgba(235, 55, 55, 0.7)"
        for v in etf_df["flow_amount"]
    ]
    fig.add_trace(
        go.Bar(
            x=etf_df["date"],
            y=etf_df["flow_amount"],
            name="設定/交換金額",
            marker_color=colors,
            hovertemplate="%{x}<br>%{customdata}<extra></extra>",
            customdata=[_format_yen(v) for v in etf_df["flow_amount"]],
        ),
        row=1, col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=etf_df["date"],
            y=etf_df["shares_outstanding"],
            name="発行済み口数",
            mode="lines",
            line=dict(color="rgba(128, 55, 235, 0.8)", width=2),
        ),
        row=2, col=1,
    )

    fig.update_layout(
        height=600,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    st.plotly_chart(fig, width="stretch")


def render_summary_metrics(daily_df: pd.DataFrame, category_label: str) -> None:
    """
    サマリー指標をメトリクスとして表示する。
    """
    if daily_df.empty:
        return

    total_creation = daily_df["total_creation"].sum()
    total_redemption = daily_df["total_redemption"].sum()
    net_flow = total_creation + total_redemption
    avg_daily = daily_df["net_flow"].mean()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="設定合計",
            value=_format_yen(total_creation),
        )
    with col2:
        st.metric(
            label="交換合計",
            value=_format_yen(total_redemption),
        )
    with col3:
        st.metric(
            label="ネットフロー",
            value=_format_yen(net_flow),
            delta=f"日平均 {_format_yen(avg_daily)}",
        )
    with col4:
        creation_days = (daily_df["total_creation"] > 0).sum()
        redemption_days = (daily_df["total_redemption"] < 0).sum()
        st.metric(
            label="設定日 / 交換日",
            value=f"{creation_days} / {redemption_days}",
        )
