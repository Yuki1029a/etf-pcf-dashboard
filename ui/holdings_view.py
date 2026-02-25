"""
個別銘柄保有残高ビュー

銘柄を選択すると、各ETFがその銘柄をどれだけ保有しているかの
時系列データを表示する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st


_ETF_COLORS = (
    px.colors.qualitative.Set2
    + px.colors.qualitative.Pastel
    + px.colors.qualitative.Set3
)


def render_holdings_view(
    holdings_df: pd.DataFrame,
    master_df: pd.DataFrame,
) -> None:
    """
    個別銘柄保有残高のメインレンダラー。
    銘柄選択 → ETFごとの保有残高時系列を表示。
    """
    if holdings_df.empty:
        st.info(
            "個別銘柄の保有残高データがありません。\n\n"
            "日次取得（daily_fetch.py）を実行すると自動で蓄積されます。"
        )
        return

    # ETF名マップ
    name_map = {}
    if not master_df.empty:
        name_map = master_df.set_index("code")["name"].to_dict()

    # --- 銘柄リスト作成（stock_code でグループ化し、最頻出の名前を使用） ---
    stock_names = _build_stock_name_map(holdings_df)
    stock_codes = sorted(stock_names.keys())

    if not stock_codes:
        st.info("個別銘柄データがありません")
        return

    # --- 銘柄検索・選択 ---
    search_text = st.text_input(
        "銘柄検索（コード or 名前）",
        placeholder="例: 7203, TOYOTA, トヨタ",
        key="holdings_stock_search",
    )

    filtered_codes = stock_codes
    if search_text:
        search_upper = search_text.upper()
        filtered_codes = [
            c for c in stock_codes
            if search_upper in c or search_upper in stock_names[c].upper()
        ]

    if not filtered_codes:
        st.warning(f"「{search_text}」に一致する銘柄が見つかりません")
        return

    stock_options = [
        f"{code}  {stock_names[code]}" for code in filtered_codes
    ]

    selected_idx = st.selectbox(
        "銘柄を選択",
        options=range(len(stock_options)),
        format_func=lambda i: stock_options[i],
        index=0,
        key="holdings_stock_select",
    )
    selected_stock = filtered_codes[selected_idx]
    selected_name = stock_names[selected_stock]

    st.subheader(f"{selected_stock} {selected_name}")

    # --- 選択銘柄のデータ抽出 ---
    stock_df = holdings_df[
        holdings_df["stock_code"] == selected_stock
    ].copy()

    if stock_df.empty:
        st.info("この銘柄のデータがありません")
        return

    # --- 最新日のETF別保有サマリー ---
    latest_date = stock_df["date"].max()
    latest = stock_df[stock_df["date"] == latest_date]

    st.caption(f"最新データ: {latest_date.strftime('%Y-%m-%d')}")

    # サマリーテーブル
    summary_rows = []
    for _, row in latest.sort_values("market_value", ascending=False).iterrows():
        etf_code = row["etf_code"]
        etf_name = name_map.get(etf_code, "")
        summary_rows.append({
            "ETFコード": etf_code,
            "ETF名": etf_name,
            "保有株数": f"{int(row['shares']):,}株",
            "株価（円）": f"{row['price']:,.1f}円",
            "時価（円）": f"{int(row['market_value']):,}円",
        })

    if summary_rows:
        st.dataframe(
            pd.DataFrame(summary_rows),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("---")

    # --- 時系列チャート（ETF別の保有残高推移） ---
    _render_holdings_chart(stock_df, selected_stock, selected_name, name_map)

    # --- 時系列テーブル ---
    _render_holdings_table(stock_df, name_map)


def _build_stock_name_map(holdings_df: pd.DataFrame) -> dict[str, str]:
    """stock_code → 最頻出の stock_name のマップを作成"""
    # 最新日のデータを使う
    latest_date = holdings_df["date"].max()
    latest = holdings_df[holdings_df["date"] == latest_date]

    name_map = {}
    for code in latest["stock_code"].unique():
        names = latest[latest["stock_code"] == code]["stock_name"]
        # 最も長い名前を採用（より正式な名前の可能性が高い）
        longest = max(names.tolist(), key=len) if not names.empty else ""
        name_map[code] = longest

    return name_map


def _render_holdings_chart(
    stock_df: pd.DataFrame,
    stock_code: str,
    stock_name: str,
    name_map: dict[str, str],
) -> None:
    """ETF別の保有残高推移チャート（積み上げ棒グラフ）"""
    st.subheader("ETF別 保有残高推移")

    # ETFごとの日次合計
    daily = stock_df.groupby(["date", "etf_code"]).agg(
        total_shares=("shares", "sum"),
        total_value=("market_value", "sum"),
    ).reset_index()

    etf_codes = sorted(daily["etf_code"].unique())
    color_map = {
        code: _ETF_COLORS[i % len(_ETF_COLORS)]
        for i, code in enumerate(etf_codes)
    }

    # 表示切替
    metric = st.radio(
        "表示指標",
        ["時価（円）", "株数"],
        horizontal=True,
        key="holdings_chart_metric",
    )
    y_col = "total_value" if metric == "時価（円）" else "total_shares"
    y_label = "時価 (円)" if metric == "時価（円）" else "株数"

    fig = go.Figure()

    for code in etf_codes:
        etf_data = daily[daily["etf_code"] == code]
        etf_label = f"{code} {name_map.get(code, '')}"
        fig.add_trace(
            go.Bar(
                x=etf_data["date"],
                y=etf_data[y_col],
                name=etf_label,
                marker_color=color_map[code],
                hovertemplate=(
                    f"{etf_label}<br>"
                    "%{x}<br>"
                    f"{y_label}: %{{y:,.0f}}<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        title=f"{stock_code} {stock_name} — ETF別保有推移",
        height=500,
        barmode="stack",
        showlegend=True,
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            xanchor="right", x=1,
        ),
        hovermode="x unified",
        yaxis_title=y_label,
        xaxis_title="日付",
    )

    st.plotly_chart(fig, use_container_width=True)


def _render_holdings_table(
    stock_df: pd.DataFrame,
    name_map: dict[str, str],
) -> None:
    """ETF別 × 日付の保有残高テーブル"""
    st.subheader("ETF別 保有残高テーブル")

    # ピボット: 行=ETF, 列=日付, 値=market_value
    pivot = stock_df.groupby(
        ["etf_code", "date"]
    )["market_value"].sum().reset_index()

    pivot_wide = pivot.pivot(
        index="etf_code", columns="date", values="market_value"
    )

    # 日付を降順に並べる
    pivot_wide = pivot_wide[sorted(pivot_wide.columns, reverse=True)]

    # ETF名を追加
    pivot_wide.insert(
        0, "ETF名",
        pivot_wide.index.map(lambda c: name_map.get(c, ""))
    )

    # 列名を日付文字列に
    pivot_wide.columns = ["ETF名"] + [
        d.strftime("%m/%d") for d in pivot_wide.columns[1:]
    ]

    # 値をフォーマット
    for col in pivot_wide.columns[1:]:
        pivot_wide[col] = pivot_wide[col].apply(
            lambda v: f"{int(v):,}円" if pd.notna(v) and v else ""
        )

    pivot_wide.index.name = "ETFコード"
    pivot_wide = pivot_wide.reset_index()

    st.dataframe(
        pivot_wide,
        use_container_width=True,
        hide_index=True,
        height=min(len(pivot_wide) * 35 + 40, 600),
    )
