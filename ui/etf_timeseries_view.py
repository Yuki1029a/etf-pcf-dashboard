"""
ETF別時系列データビュー - Excel風の一覧表示

個別ETFを選択すると、日付ごとのNAV・口数・現金・株式・先物等を
Excel形式に近い表形式で表示する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import streamlit as st


def _fmt_yen(value) -> str:
    """金額をカンマ区切り + 円で表示（丸めなし）"""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return f"{int(value):,}円"


def _fmt_int(value) -> str:
    """整数をカンマ区切りで表示"""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return f"{int(value):,}"


def _fmt_shares(value) -> str:
    """枚数をカンマ区切り + 枚で表示"""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return f"{int(value):,}枚"


def render_etf_timeseries(
    ts_df: pd.DataFrame,
    master_df: pd.DataFrame,
) -> None:
    """
    ETF別時系列テーブルのメインレンダラー。
    ETF選択 → 日付ごとの詳細一覧を表示。
    """
    if ts_df.empty:
        st.info("時系列データがありません")
        return

    # --- ETF選択 ---
    etf_codes = sorted(ts_df["etf_code"].unique())

    # マスタからETF名を取得してラベル作成
    name_map = {}
    if not master_df.empty:
        name_map = master_df.set_index("code")["name"].to_dict()

    etf_options = [
        f"{code}  {name_map.get(code, '')}" for code in etf_codes
    ]

    selected_idx = st.selectbox(
        "ETFを選択",
        options=range(len(etf_options)),
        format_func=lambda i: etf_options[i],
        index=0,
        key="etf_ts_select",
    )
    selected_code = etf_codes[selected_idx]

    # --- 対象ETFのデータ抽出 ---
    etf_df = ts_df[ts_df["etf_code"] == selected_code].copy()
    etf_df = etf_df.sort_values("date", ascending=False)

    if etf_df.empty:
        st.info(f"{selected_code} のデータがありません")
        return

    etf_name = name_map.get(selected_code, "")
    st.subheader(f"{selected_code} {etf_name}")

    # --- サマリーメトリクス ---
    latest = etf_df.iloc[0]
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        nav = latest["nav"]
        st.metric("NAV（純資産）", _fmt_yen(nav) if pd.notna(nav) else "---")
    with col2:
        so = latest["shares_outstanding"]
        st.metric("発行済口数", f"{int(so):,}口" if pd.notna(so) else "---")
    with col3:
        npu = latest.get("nav_per_unit")
        st.metric("1口あたりNAV", f"{npu:,.2f}円" if pd.notna(npu) else "---")
    with col4:
        st.metric("データ日数", f"{len(etf_df)} 日")

    # --- 先物情報（あれば） ---
    has_futures = etf_df["futures1_type"].notna().any()
    if has_futures:
        ft = latest.get("futures1_type", "")
        fq = latest.get("futures1_quantity")
        fm = latest.get("futures1_contract_month", "")
        futures_label = f"{ft} {fm}  {_fmt_shares(fq)}" if ft else ""

        ft2 = latest.get("futures2_type", "")
        if pd.notna(ft2) and ft2:
            fq2 = latest.get("futures2_quantity")
            fm2 = latest.get("futures2_contract_month", "")
            futures_label += f"  /  {ft2} {fm2}  {_fmt_shares(fq2)}"

        if futures_label:
            st.caption(f"先物ポジション（最新）: {futures_label}")

    st.markdown("---")

    # --- 時系列テーブル構築 ---
    display_df = _build_display_table(etf_df, has_futures)

    # テーブル表示
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=min(len(display_df) * 35 + 40, 800),
    )

    # --- CSV ダウンロード ---
    csv_data = _build_csv_export(etf_df, selected_code)
    st.download_button(
        label="CSV ダウンロード",
        data=csv_data,
        file_name=f"pcf_{selected_code}.csv",
        mime="text/csv",
    )


def _build_display_table(
    etf_df: pd.DataFrame,
    has_futures: bool,
) -> pd.DataFrame:
    """表示用のDataFrameを構築する"""
    rows = []
    for _, row in etf_df.iterrows():
        d = {
            "日付": row["date"].strftime("%Y-%m-%d"),
            "NAV（円）": _fmt_yen(row["nav"]),
            "1口NAV（円）": f"{row['nav_per_unit']:,.2f}円" if pd.notna(row.get("nav_per_unit")) else "",
            "発行済口数": _fmt_int(row["shares_outstanding"]),
            "現金（円）": _fmt_yen(row["cash_component"]),
            "株式残高（円）": _fmt_yen(row["equity_market_value"]),
        }

        if has_futures:
            # 先物1
            ft1 = row.get("futures1_type", "")
            if pd.notna(ft1) and ft1:
                fq1 = row.get("futures1_quantity")
                fm1 = row.get("futures1_contract_month", "")
                fmv1 = row.get("futures1_market_value")
                fmult1 = row.get("futures1_multiplier", 1) or 1

                # 想定元本を計算
                notional1 = _calc_notional(fmv1, fq1, fmult1)

                d["先物1"] = f"{ft1} {fm1}" if fm1 else ft1
                d["先物1枚数"] = _fmt_shares(fq1) if pd.notna(fq1) else ""
                d["先物1想定元本（円）"] = _fmt_yen(notional1) if notional1 else ""
            else:
                d["先物1"] = ""
                d["先物1枚数"] = ""
                d["先物1想定元本（円）"] = ""

            # 先物2
            ft2 = row.get("futures2_type", "")
            if pd.notna(ft2) and ft2:
                fq2 = row.get("futures2_quantity")
                fm2 = row.get("futures2_contract_month", "")
                fmv2 = row.get("futures2_market_value")
                fmult2 = row.get("futures2_multiplier", 1) or 1

                notional2 = _calc_notional(fmv2, fq2, fmult2)

                d["先物2"] = f"{ft2} {fm2}" if fm2 else ft2
                d["先物2枚数"] = _fmt_shares(fq2) if pd.notna(fq2) else ""
                d["先物2想定元本（円）"] = _fmt_yen(notional2) if notional2 else ""

        rows.append(d)

    return pd.DataFrame(rows)


def _calc_notional(mv, qty, mult) -> float:
    """想定元本を計算（aggregatorと同じロジック）"""
    if mv is None or (isinstance(mv, float) and np.isnan(mv)) or not mv:
        return 0.0
    if qty is None or (isinstance(qty, float) and np.isnan(qty)) or not qty:
        return mv
    if mult is None or (isinstance(mult, float) and np.isnan(mult)) or not mult:
        return mv
    unit_est = abs(mv) / (abs(qty) * mult)
    if unit_est >= 100:
        return mv  # 既に想定元本
    else:
        return mv * mult  # multiplier適用


def _build_csv_export(etf_df: pd.DataFrame, etf_code: str) -> str:
    """CSV エクスポート用の文字列を生成"""
    export_cols = [
        "date", "nav", "nav_per_unit", "shares_outstanding",
        "cash_component", "equity_market_value", "equity_count_tse",
    ]

    # 先物があれば追加
    if etf_df["futures1_type"].notna().any():
        export_cols += [
            "futures1_type", "futures1_contract_month",
            "futures1_quantity", "futures1_market_value", "futures1_multiplier",
        ]
    if etf_df["futures2_type"].notna().any():
        export_cols += [
            "futures2_type", "futures2_contract_month",
            "futures2_quantity", "futures2_market_value", "futures2_multiplier",
        ]

    # 存在するカラムのみ
    export_cols = [c for c in export_cols if c in etf_df.columns]

    export_df = etf_df[export_cols].copy()
    export_df = export_df.sort_values("date", ascending=False)

    return export_df.to_csv(index=False)
