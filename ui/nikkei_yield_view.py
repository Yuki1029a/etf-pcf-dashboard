"""
日経225採用銘柄 予想配当利回り（日経予想）ビュー
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def render_nikkei_yield_view(yield_df: pd.DataFrame) -> None:
    if yield_df.empty:
        st.info(
            "予想配当利回りデータがありません。\n\n"
            "`python scripts/fetch_nikkei_yield.py` を実行すると蓄積されます。"
        )
        return

    df = yield_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    dates = sorted(df["date"].dt.date.unique(), reverse=True)

    # --- 日次サマリー ---
    daily = (
        df.dropna(subset=["nikkei_yield"])
        .groupby("date")["nikkei_yield"]
        .agg(平均="mean", 中央値="median", 銘柄数="count")
        .reset_index()
    )
    latest = daily.iloc[-1]
    c1, c2, c3 = st.columns(3)
    c1.metric("最新日", latest["date"].strftime("%Y-%m-%d"))
    c2.metric("単純平均利回り", f"{latest['平均']:.2f}%")
    c3.metric("取得銘柄数", f"{int(latest['銘柄数'])} / 225")

    if len(daily) > 1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=daily["date"], y=daily["平均"], name="単純平均", mode="lines+markers"))
        fig.add_trace(go.Scatter(x=daily["date"], y=daily["中央値"], name="中央値", mode="lines+markers"))
        fig.update_layout(
            title="採用銘柄 予想配当利回り（日経予想）推移", yaxis_title="%",
            height=320, margin=dict(t=40, b=20), hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

    # --- 日付選択 → 銘柄別一覧 ---
    sel = st.selectbox("日付", options=dates, index=0, key="nikkei_yield_date")
    day = df[df["date"].dt.date == sel].copy()

    search = st.text_input(
        "銘柄検索（コード or 名前）", placeholder="例: 7203, TOYOTA", key="nikkei_yield_search",
    )
    if search:
        s = search.upper()
        day = day[day["stock_code"].str.contains(s) | day["stock_name"].str.upper().str.contains(s)]

    day = day.sort_values("nikkei_yield", ascending=False, na_position="last")
    show = day[["stock_code", "stock_name", "nikkei_yield", "as_of", "status"]].rename(columns={
        "stock_code": "コード", "stock_name": "銘柄名", "nikkei_yield": "予想配当利回り(%)",
        "as_of": "基準日", "status": "状態",
    })
    st.dataframe(show, use_container_width=True, hide_index=True, height=600)

    # --- 個別銘柄の推移 ---
    if len(dates) > 1:
        codes = sorted(df["stock_code"].unique())
        names = df.drop_duplicates("stock_code").set_index("stock_code")["stock_name"].to_dict()
        pick = st.selectbox(
            "個別銘柄の推移", options=codes,
            format_func=lambda c: f"{c} {names.get(c, '')}", key="nikkei_yield_code",
        )
        one = df[df["stock_code"] == pick].sort_values("date")
        fig2 = go.Figure(go.Scatter(x=one["date"], y=one["nikkei_yield"], mode="lines+markers"))
        fig2.update_layout(
            title=f"{pick} {names.get(pick, '')} 予想配当利回り", yaxis_title="%",
            height=300, margin=dict(t=40, b=20),
        )
        st.plotly_chart(fig2, use_container_width=True)
