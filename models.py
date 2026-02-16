"""
PCFデータ自動集計・分析システム - データモデル定義
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


@dataclass
class ETFMaster:
    """ETFマスタ情報"""
    code: str                           # "1306", "2640", "380A"
    name: str = ""                      # ETF名称
    provider: str = "ice"               # "ice", "solactive", "spglobal"
    category: str = "other"             # "topix", "nikkei225", "other"
    has_futures: bool = False
    listing_date: Optional[date] = None


@dataclass
class FuturesPosition:
    """正規化された先物ポジション"""
    raw_name: str                       # 生データの銘柄名 (例: "TOPIX 2603")
    futures_type: str                   # 正規化後の種類 (例: "TOPIX", "NK225_MINI")
    contract_month: Optional[str]       # 限月 YYMM形式 (例: "2603")
    quantity: int                       # 枚数 (正=買い, 負=売り)
    market_value: float                 # 時価評価 (CSVの値)
    multiplier: int = 1                 # 掛け目


@dataclass
class PCFRecord:
    """1日1ETFのPCF集計レコード（Excelの1行に対応）"""
    etf_code: str
    pcf_date: date
    nav: Optional[float] = None                 # ファンド純資産総額
    shares_outstanding: Optional[int] = None     # 発行済み口数
    cash_component: Optional[float] = None       # ファンド内現金
    equity_count_tse: Optional[int] = None       # 現物株数 (TSE)
    equity_market_value: Optional[float] = None  # 現物株残高 (TSE)

    # 先物ポジション（最大3つ）
    futures_positions: list[FuturesPosition] = field(default_factory=list)

    @property
    def nav_per_unit(self) -> Optional[float]:
        """1口あたりNAV"""
        if self.nav and self.shares_outstanding and self.shares_outstanding > 0:
            return self.nav / self.shares_outstanding
        return None


@dataclass
class CreationRedemption:
    """設定・交換の日次レコード"""
    etf_code: str
    trade_date: date
    shares_change: int              # 口数増減 (正=設定, 負=交換)
    nav_per_unit: float             # 1口あたりNAV
    flow_amount: float              # 設定・交換金額 = 口数増減 × 1口あたりNAV
    flow_type: str                  # "creation" or "redemption"
