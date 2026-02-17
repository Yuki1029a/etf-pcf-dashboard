"""
PCFデータ自動集計・分析システム - 設定・定数
"""
from pathlib import Path

# ============================================================
# パス設定
# ============================================================
PROJECT_ROOT = Path(__file__).parent
CACHE_DIR = PROJECT_ROOT / "cache"
STORE_DIR = PROJECT_ROOT / "store"

# 既存Excelファイル
EXCEL_PATH = Path(r"H:\マイドライブ\bank\pcf_getter (2).xlsm")

# ストアファイル
ETF_TIMESERIES_PATH = STORE_DIR / "etf_timeseries.parquet"
ETF_MASTER_PATH = STORE_DIR / "etf_master.csv"
FUTURES_POSITIONS_PATH = STORE_DIR / "futures_positions.parquet"

# ============================================================
# PCFプロバイダ URL
# ============================================================
ICE_PCF_URL = "https://inav.ice.com/pcf-download/{code}.csv"
SOLACTIVE_SINGLE_URL = "https://www.solactive.com/downloads/etfservices/tse-pcf/single/{code}.csv"
SOLACTIVE_BULK_URL = "https://www.solactive.com/downloads/etfservices/tse-pcf/bulk/{yyyy}/{mm}/{dd}.zip"

# S&P Global (ebs.ihsmarkit.com) API
SPGLOBAL_API_BASE = "https://api.ebs.ihsmarkit.com/inav/"
SPGLOBAL_FILEDATES_URL = SPGLOBAL_API_BASE + "filedates"
SPGLOBAL_DATA_URL = SPGLOBAL_API_BASE + "data"
SPGLOBAL_FILE_URL = SPGLOBAL_API_BASE + "getfile"

# S&P Global リクエストヘッダー (CORS対応)
SPGLOBAL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, */*",
    "Origin": "https://ebs.ihsmarkit.com",
    "Referer": "https://ebs.ihsmarkit.com/inav/",
}

# ============================================================
# Excelシート構造（ヘッダー名 → 列インデックス）
# ============================================================
EXCEL_COLUMNS = {
    "日付": 0,
    "NAV": 1,
    "発行済み口数": 2,
    "ファンド内現金": 3,
    "現物株数_TSE": 4,
    "現物株残高_TSE": 5,
    "先物の種類1": 6,
    "建玉枚数1": 7,
    "先物残高1": 8,
    "先物の種類2": 9,
    "建玉枚数2": 10,
    "先物残高2": 11,
}

# Excelの特殊シート（ETFデータではないシート）
EXCEL_SPECIAL_SHEETS = {
    "目次_TOP", "目次_BOTTOM", "グラフ", "Sheet1", "エラーログ",
}

# ============================================================
# ETF分類マッピング
# ソース: https://www.jpx.co.jp/equities/products/etfs/issues/01.html
#         https://www.jpx.co.jp/equities/products/etfs/leveraged-inverse/01.html
# ============================================================

# --- TOPIX 本体連動 ---
TOPIX_PLAIN_CODES = [
    "1305",  # iFreeETF TOPIX（年1回決算型）
    "1306",  # NEXT FUNDS TOPIX連動型上場投信
    "1308",  # 上場インデックスファンドTOPIX
    "1348",  # MAXIS トピックス上場投信
    "1473",  # One ETF トピックス
    "1475",  # iシェアーズ・コア TOPIX ETF
    "2524",  # NZAM 上場投信 TOPIX
    "2557",  # SMDAM トピックス上場投信
    "2625",  # iFreeETF TOPIX（年4回決算型）
]

# --- TOPIX レバレッジ（ブル） ---
TOPIX_LEV_CODES = [
    "1568",  # TOPIXブル2倍上場投信
    "1367",  # iFreeETF TOPIXレバレッジ（2倍）指数
]

# --- TOPIX インバース（ベア） ---
TOPIX_INV_CODES = [
    "1569",  # TOPIXベア上場投信
    "1457",  # iFreeETF TOPIXインバース（-1倍）指数
    "1356",  # TOPIXベア2倍上場投信
    "1368",  # iFreeETF TOPIXダブルインバース（-2倍）指数
]

# 後方互換
TOPIX_LEVINV_CODES = TOPIX_LEV_CODES + TOPIX_INV_CODES

# --- 日経225 本体連動 ---
NIKKEI225_PLAIN_CODES = [
    "1320",  # iFreeETF 日経225（年1回決算型）
    "1321",  # NEXT FUNDS 日経225連動型上場投信
    "1329",  # iシェアーズ・コア 日経225 ETF
    "1330",  # 上場インデックスファンド225
    "1346",  # MAXIS 日経225上場投信
    "1369",  # One ETF 日経225
    "1397",  # SMDAM 日経225上場投信
    "1578",  # 上場インデックスファンド日経225（ミニ）
    "2525",  # NZAM 上場投信 日経225
    "2624",  # iFreeETF 日経225（年4回決算型）
    "473A",  # ニッセイETF 日経225インデックス
]

# --- 日経225 レバレッジ（ブル） ---
NIKKEI225_LEV_CODES = [
    "1358",  # 上場インデックスファンド日経レバレッジ指数
    "1365",  # iFreeETF 日経平均レバレッジ・インデックス
    "1458",  # 楽天ETF-日経レバレッジ指数連動型
    "1570",  # NEXT FUNDS 日経平均レバレッジ・インデックス連動型上場投信
    "1579",  # 日経平均ブル2倍上場投信
]

# --- 日経225 インバース（ベア） ---
NIKKEI225_INV_CODES = [
    "1456",  # iFreeETF 日経平均インバース・インデックス
    "1571",  # NEXT FUNDS 日経平均インバース・インデックス連動型上場投信
    "1580",  # 日経平均ベア上場投信
    "1357",  # NEXT FUNDS 日経平均ダブルインバース・インデックス連動型上場投信
    "1360",  # 日経平均ベア2倍上場投信
    "1366",  # iFreeETF 日経平均ダブルインバース・インデックス
    "1459",  # 楽天ETF-日経ダブルインバース指数連動型
]

# 後方互換
NIKKEI225_LEVINV_CODES = NIKKEI225_LEV_CODES + NIKKEI225_INV_CODES

# 後方互換: 全TOPIX / 全225（本体 + レバインバ）
TOPIX_ETF_CODES = TOPIX_PLAIN_CODES + TOPIX_LEVINV_CODES
NIKKEI225_ETF_CODES = NIKKEI225_PLAIN_CODES + NIKKEI225_LEVINV_CODES

# カテゴリ → コードリスト のマッピング（UI用）
CATEGORY_CODE_MAP: dict[str, list[str]] = {
    "topix": TOPIX_PLAIN_CODES,
    "topix_lev": TOPIX_LEV_CODES,
    "topix_inv": TOPIX_INV_CODES,
    "topix_levinv": TOPIX_LEVINV_CODES,
    "topix_all": TOPIX_ETF_CODES,
    "nikkei225": NIKKEI225_PLAIN_CODES,
    "nikkei225_lev": NIKKEI225_LEV_CODES,
    "nikkei225_inv": NIKKEI225_INV_CODES,
    "nikkei225_levinv": NIKKEI225_LEVINV_CODES,
    "nikkei225_all": NIKKEI225_ETF_CODES,
}

# カテゴリの表示名
CATEGORY_LABELS: dict[str, str] = {
    "topix": "TOPIX型（本体）",
    "topix_lev": "TOPIX型（レバレッジ）",
    "topix_inv": "TOPIX型（インバース）",
    "topix_levinv": "TOPIX型（レバ・インバ）",
    "topix_all": "TOPIX型（全体）",
    "nikkei225": "日経225型（本体）",
    "nikkei225_lev": "日経225型（レバレッジ）",
    "nikkei225_inv": "日経225型（インバース）",
    "nikkei225_levinv": "日経225型（レバ・インバ）",
    "nikkei225_all": "日経225型（全体）",
    "all": "全ETF",
}

# ============================================================
# 先物掛け目（乗数）
# ============================================================
FUTURES_MULTIPLIERS: dict[str, int] = {
    "TOPIX": 10_000,
    "MINI_TOPIX": 1_000,
    "NK225": 1_000,
    "NK225_MINI": 100,
    "NK225_MICRO": 10,
    "JPX400": 100,
    "TSEREIT": 1_000,
    "JGB10Y": 10_000,
    "TOPIX_BANKS": 1_000,        # TOPIX Banks Index 先物
    "TOPIX_CORE30": 10_000,      # Core30 先物
    "TSE_GROWTH": 1_000,         # TSE Growth Market 250 先物
    "JPX_PRIME150": 1_000,       # JPX Prime 150 先物
    "NK225_OPTION_CALL": 1_000,  # 日経225オプション (コール)
    "NK225_OPTION_PUT": 1_000,   # 日経225オプション (プット)
    "UNKNOWN": 1,                # 不明な先物 (フォールバック)
}
