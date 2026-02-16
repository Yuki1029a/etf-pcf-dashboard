"""
サイドバー フィルタ UI
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from config import CATEGORY_CODE_MAP, CATEGORY_LABELS


def render_sidebar(master_df: pd.DataFrame) -> dict:
    """
    サイドバーにフィルタを表示し、選択値を返す。

    Returns:
        dict: {
            "category": str,
            "date_from": date,
            "date_to": date,
            "selected_etf": Optional[str],
        }
    """
    st.sidebar.title("PCF データ分析")
    st.sidebar.markdown("---")

    # カテゴリ選択
    category_options = {
        "TOPIX型（本体）": "topix",
        "TOPIX型（レバレッジ）": "topix_lev",
        "TOPIX型（インバース）": "topix_inv",
        "日経225型（本体）": "nikkei225",
        "日経225型（レバレッジ）": "nikkei225_lev",
        "日経225型（インバース）": "nikkei225_inv",
        "全ETF": "all",
    }
    selected_label = st.sidebar.radio(
        "ETFカテゴリ",
        options=list(category_options.keys()),
        index=0,
    )
    category = category_options[selected_label]

    st.sidebar.markdown("---")

    # 日付範囲
    today = date.today()
    default_from = today - timedelta(days=90)

    col1, col2 = st.sidebar.columns(2)
    with col1:
        date_from = st.date_input("開始日", value=default_from)
    with col2:
        date_to = st.date_input("終了日", value=today)

    st.sidebar.markdown("---")

    # 個別ETF選択（オプション）
    selected_etf = None
    if not master_df.empty:
        if category in CATEGORY_CODE_MAP:
            etf_list = [
                c for c in CATEGORY_CODE_MAP[category]
                if c in master_df["code"].values
            ]
        else:
            etf_list = master_df["code"].tolist()

        etf_list = sorted(etf_list)
        etf_options = ["(全ETF合計)"] + etf_list

        selected = st.sidebar.selectbox(
            "個別ETF選択",
            options=etf_options,
            index=0,
        )
        if selected != "(全ETF合計)":
            selected_etf = selected

    # 統計情報
    st.sidebar.markdown("---")
    st.sidebar.markdown("### データ概要")
    if not master_df.empty:
        total = len(master_df)
        futures_count = master_df["has_futures"].sum()
        st.sidebar.markdown(f"- 総ETF数: **{total}**")
        for key, codes in CATEGORY_CODE_MAP.items():
            if key.endswith("_all") or key == "all":
                continue
            label = CATEGORY_LABELS.get(key, key)
            count = len([c for c in codes if c in master_df["code"].values])
            st.sidebar.markdown(f"- {label}: **{count}**")
        st.sidebar.markdown(f"- 先物保有: **{int(futures_count)}**")

    return {
        "category": category,
        "date_from": date_from,
        "date_to": date_to,
        "selected_etf": selected_etf,
    }
