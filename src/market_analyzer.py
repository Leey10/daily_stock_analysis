# -*- coding: utf-8 -*-
"""
美股大盘复盘分析模块
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any, List

import yfinance as yf

from src.config import get_config
from src.search_service import SearchService

logger = logging.getLogger(__name__)


@dataclass
class MarketIndex:
    code: str
    name: str
    current: float = 0.0
    change: float = 0.0
    change_pct: float = 0.0
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    prev_close: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__


@dataclass
class MarketOverview:
    date: str
    indices: List[MarketIndex] = field(default_factory=list)


class MarketAnalyzer:
    """美股大盘分析器"""

    MAIN_INDICES = {
        "^GSPC": "S&P 500",
        "^IXIC": "NASDAQ Composite",
        "^DJI": "Dow Jones",
        "^VIX": "VIX Volatility",
    }

    def __init__(self, search_service: Optional[SearchService] = None, analyzer=None):
        self.config = get_config()
        self.search_service = search_service
        self.analyzer = analyzer

    # ================= 获取指数行情 =================
    def _get_main_indices(self) -> List[MarketIndex]:
        indices = []
        logger.info("[大盘] 获取美股主要指数行情...")

        for code, name in self.MAIN_INDICES.items():
            try:
                ticker = yf.Ticker(code)
                hist = ticker.history(period="2d")
                if hist.empty:
                    continue

                today = hist.iloc[-1]
                prev = hist.iloc[-2] if len(hist) > 1 else today

                price = float(today["Close"])
                prev_close = float(prev["Close"])
                change = price - prev_close
                change_pct = (change / prev_close) * 100 if prev_close else 0

                indices.append(
                    MarketIndex(
                        code=code,
                        name=name,
                        current=price,
                        change=change,
                        change_pct=change_pct,
                        open=float(today["Open"]),
                        high=float(today["High"]),
                        low=float(today["Low"]),
                        prev_close=prev_close,
                    )
                )
            except Exception as e:
                logger.warning(f"[大盘] 获取 {name} 失败: {e}")

        return indices

    # ================= 市场概览 =================
    def get_market_overview(self) -> MarketOverview:
        today = datetime.now().strftime("%Y-%m-%d")
        overview = MarketOverview(date=today)
        overview.indices = self._get_main_indices()
        return overview

    # ================= 新闻 =================
    def search_market_news(self) -> List[Dict]:
        if not self.search_service:
            return []

        queries = [
            "US stock market news today",
            "Wall Street market recap",
            "NASDAQ S&P500 market analysis",
        ]

        news = []
        for q in queries:
            try:
                r = self.search_service.search_stock_news(
                    stock_code="market",
                    stock_name="US Market",
                    max_results=3,
                    focus_keywords=q.split(),
                )
                if r and r.results:
                    news.extend(r.results)
            except:
                pass

        return news

    # ================= 报告生成 =================
    def generate_market_review(self, overview: MarketOverview, news: List) -> str:
        indices_text = "\n".join(
            [f"- {i.name}: {i.current:.2f} ({i.change_pct:+.2f}%)" for i in overview.indices]
        )

        news_text = "\n".join(
            [f"{n.title[:60]}" if hasattr(n, "title") else n.get("title", "")[:60] for n in news[:5]]
        )

        prompt = f"""
You are a professional US stock market analyst.

Market Date: {overview.date}

Indices:
{indices_text}

News:
{news_text}

Write a concise US market recap in Markdown.
"""

        if not self.analyzer or not self.analyzer.is_available():
            return f"### US Market Recap ({overview.date})\n{indices_text}"

        return self.analyzer._call_openai_api(prompt, {"temperature": 0.7, "max_output_tokens": 1500})

    # ================= 主流程 =================
    def run_daily_review(self) -> str:
        logger.info("========== 开始美股大盘复盘 ==========")
        overview = self.get_market_overview()
        news = self.search_market_news()
        report = self.generate_market_review(overview, news)
        logger.info("========== 美股大盘复盘完成 ==========")
        return report
