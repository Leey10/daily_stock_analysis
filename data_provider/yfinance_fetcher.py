# -*- coding: utf-8 -*-
"""
===================================
YfinanceFetcher - 美股专用数据源
===================================

数据来源：Yahoo Finance（yfinance）
用途：美股股票 / ETF 历史行情
定位：美股唯一行情源（已移除 A 股支持）
"""

import logging
import re
from typing import Optional

import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_exponential

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS

logger = logging.getLogger(__name__)


class YfinanceFetcher(BaseFetcher):
    name = "YfinanceFetcher"
    priority = 0  # 设为最高优先级

    def __init__(self):
        pass

    # ===============================
    # 美股代码识别（核心修复点）
    # ===============================
    def _validate_us_stock_code(self, stock_code: str) -> str:
        """
        只允许合法美股代码：
        MSFT, AAPL, TSLA, SPY, QQQ, BRK.B
        """
        code = stock_code.strip().upper()

        if re.match(r"^[A-Z]{1,6}(\.[A-Z]{1,2})?$", code):
            return code

        raise DataFetchError(f"非法美股代码: {stock_code}")

    # ===============================
    # 获取历史行情
    # ===============================
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        yf_code = self._validate_us_stock_code(stock_code)

        logger.info(f"[YFinance] 下载 {yf_code} 数据: {start_date} → {end_date}")

        df = yf.download(
            tickers=yf_code,
            start=start_date,
            end=end_date,
            progress=False,
            auto_adjust=True,
        )

        if df is None or df.empty:
            raise DataFetchError(f"Yahoo Finance 无数据: {yf_code}")

        return df

    # ===============================
    # 数据标准化（修复 MA None 问题）
    # ===============================
    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        if df is None or df.empty:
            raise DataFetchError(f"标准化失败，数据为空: {stock_code}")

        df = df.reset_index()

        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }, inplace=True)

        df["date"] = pd.to_datetime(df["date"])
        df["pct_chg"] = df["close"].pct_change() * 100
        df["pct_chg"] = df["pct_chg"].fillna(0).round(2)
        df["amount"] = df["volume"] * df["close"]
        df["code"] = stock_code.upper()

        keep_cols = ["code"] + STANDARD_COLUMNS
        df = df[[c for c in keep_cols if c in df.columns]]

        return df
