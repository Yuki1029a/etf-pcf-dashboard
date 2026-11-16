"""
PCF データ分析ダッシュボード - Streamlit メインエントリ

Usage:
    streamlit run app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent))

from config import CATEGORY_CODE_MAP, CATEGORY_LABELS
from data.storage import (
    load_timeseries, load_etf_master, load_holdings, load_nikkei_yield,
)
from data.aggregator import (
    compute_creation_redemption,
    aggregate_by_category,
    aggregate_by_etf,
    aggregate_etf_breakdown,
    get_daily_ranking,
    aggregate_nav_total,
    aggregate_nav_etf_breakdown,
    compute_futures_exposure,
)
from ui.sidebar import render_sidebar
from ui.creation_redemption import (
    render_creation_redemption_chart,
    render_etf_detail_chart,
    render_summary_metrics,
    render_daily_ranking,
)
from ui.nav_view import render_nav_timeseries
from ui.futures_view import render_futures_analysis
from ui.etf_timeseries_view import render_etf_timeseries
from ui.holdings_view import render_holdings_view
from ui.nikkei_yield_view import render_nikkei_yield_view
from data.index_data import fetch_index_data


# ============================================================
# ページ設定
# ============================================================
st.set_page_config(
    page_title="ETF PCF 分析ダッシュボード",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# データ読み込み（キャッシュ）
# ============================================================
@st.cache_data(ttl=300)
def load_data():
    """時系列データとマスタを読み込む"""
    ts_df = load_timeseries()
    master_df = load_etf_master()
    holdings_df = load_holdings()
    yield_df = load_nikkei_yield()
    return ts_df, master_df, holdings_df, yield_df


@st.cache_data(ttl=3600)
def load_index_data(date_from, date_to):
    """日経平均・TOPIXの指数データを取得"""
    return fetch_index_data(date_from, date_to)


def _render_category_section(cr_df, category, master_df, index_df=None):
    """カテゴリ1つ分の設定・交換セクションを描画する"""
    label = CATEGORY_LABELS.get(category, category)
    st.header(f"{label} の設定・交換")

    daily = aggregate_by_category(cr_df, category, master_df)
    if daily.empty:
        st.info(f"{label} のデータがありません")
        return

    render_summary_metrics(daily, label)

    # カテゴリに応じた指数を選択
    idx = None
    if index_df is not None and not index_df.empty:
        if "topix" in category:
            cols = ["date", "TOPIX"] if "TOPIX" in index_df.columns else []
        else:
            cols = ["date", "日経平均"] if "日経平均" in index_df.columns else []
        if cols:
            idx = index_df[cols].dropna()

    # ETF別内訳付き棒グラフ + 指数二軸
    breakdown = aggregate_etf_breakdown(cr_df, category, master_df)
    render_creation_redemption_chart(
        daily, f"{label} 設定・交換", etf_breakdown=breakdown, index_df=idx
    )

    # 日付選択 → 日次ランキング
    dates = sorted(daily["date"].dt.date.tolist(), reverse=True)
    selected_date = st.selectbox(
        "日付を選択してETF別ランキングを表示",
        options=dates,
        index=0,
        key=f"date_select_{category}",
    )
    if selected_date:
        ranking = get_daily_ranking(cr_df, selected_date, category, master_df)
        st.subheader(f"{selected_date} の設定・交換ランキング")
        render_daily_ranking(ranking, selected_date)


def main():
    # データ読み込み
    ts_df, master_df, holdings_df, yield_df = load_data()

    if ts_df.empty:
        st.error(
            "データが見つかりません。\n\n"
            "先に以下のコマンドでExcelデータを移行してください:\n\n"
            "```\n"
            "python scripts/import_excel.py\n"
            "```"
        )
        return

    # サイドバー
    filters = render_sidebar(master_df)
    category = filters["category"]
    date_from = filters["date_from"]
    date_to = filters["date_to"]
    selected_etf = filters["selected_etf"]

    # 日付フィルタ適用
    filtered_df = ts_df[
        (ts_df["date"] >= pd.Timestamp(date_from))
        & (ts_df["date"] <= pd.Timestamp(date_to))
    ].copy()

    # 指数データ取得（設定・交換 + 資産残高で共用）
    index_df = load_index_data(date_from, date_to)

    # タブ構成
    tab_cr, tab_nav, tab_futures, tab_data, tab_holdings, tab_yield = st.tabs([
        "📈 設定・交換", "💰 資産残高", "📊 先物分析",
        "📋 データ一覧", "🏢 個別銘柄", "💹 日経予想利回り",
    ])

    # ========================================
    # タブ1: 設定・交換
    # ========================================
    with tab_cr:
        cr_df = compute_creation_redemption(filtered_df)

        if selected_etf:
            st.header(f"ETF {selected_etf} の設定・交換")
            etf_cr = aggregate_by_etf(cr_df, selected_etf)
            if not etf_cr.empty:
                render_etf_detail_chart(etf_cr, selected_etf)
            else:
                st.info(f"{selected_etf} の設定・交換データがありません")
        else:
            if category == "all":
                for cat_key in [
                    "topix", "topix_lev", "topix_inv",
                    "nikkei225", "nikkei225_lev", "nikkei225_inv",
                ]:
                    _render_category_section(cr_df, cat_key, master_df, index_df)
                    st.markdown("---")
            else:
                _render_category_section(cr_df, category, master_df, index_df)

    # ========================================
    # タブ2: 資産残高
    # ========================================
    with tab_nav:
        st.header("資産残高（NAV合計）")

        def _get_index_for_category(cat_key, index_df):
            """カテゴリに応じた指数データを返す"""
            if index_df is None or index_df.empty:
                return None
            if "topix" in cat_key:
                cols = ["date", "TOPIX"] if "TOPIX" in index_df.columns else []
            else:
                cols = ["date", "日経平均"] if "日経平均" in index_df.columns else []
            return index_df[cols].dropna() if cols else None

        if category == "all":
            for cat_key in [
                "topix", "topix_lev", "topix_inv",
                "nikkei225", "nikkei225_lev", "nikkei225_inv",
            ]:
                cat_label = CATEGORY_LABELS.get(cat_key, cat_key)
                nav_data = aggregate_nav_total(filtered_df, cat_key, master_df)
                if not nav_data.empty:
                    breakdown = aggregate_nav_etf_breakdown(
                        filtered_df, cat_key, master_df
                    )
                    idx = _get_index_for_category(cat_key, index_df)
                    render_nav_timeseries(
                        nav_data, f"{cat_label} 資産残高",
                        index_df=idx, etf_breakdown=breakdown,
                    )
                    st.markdown("---")
        else:
            label = CATEGORY_LABELS.get(category, category)
            nav_data = aggregate_nav_total(filtered_df, category, master_df)
            if not nav_data.empty:
                breakdown = aggregate_nav_etf_breakdown(
                    filtered_df, category, master_df
                )
                idx = _get_index_for_category(category, index_df)
                render_nav_timeseries(
                    nav_data, f"{label} 資産残高",
                    index_df=idx, etf_breakdown=breakdown,
                )
            else:
                st.info("資産残高データがありません")

    # ========================================
    # タブ3: 先物分析
    # ========================================
    with tab_futures:
        st.header("先物ポジション分析")
        futures_df = compute_futures_exposure(filtered_df)
        render_futures_analysis(futures_df, index_df=index_df)

    # ========================================
    # タブ4: データ一覧
    # ========================================
    with tab_data:
        st.header("ETF別 時系列データ")
        render_etf_timeseries(filtered_df)

    # ========================================
    # タブ5: 個別銘柄
    # ========================================
    with tab_holdings:
        st.header("個別銘柄 保有残高")
        render_holdings_view(holdings_df)

    # ========================================
    # タブ6: 日経225 予想配当利回り（日経予想）
    # ========================================
    with tab_yield:
        st.header("日経225採用銘柄 予想配当利回り（日経予想）")
        render_nikkei_yield_view(yield_df)


if __name__ == "__main__":
    main()
