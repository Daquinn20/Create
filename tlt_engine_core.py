"""
TLT engine core — analytical classes with no Streamlit dependencies.

Shared by:
  - Technical_Screen_Quinn.py (interactive Streamlit app)
  - tlt_signal_tracker.py (headless daily capture)

Single source of truth for signal classification so the dashboard and the
persisted signal history can never diverge.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY")


# ============================================================================
# TECHNICAL INDICATORS
# ============================================================================

class TechnicalIndicators:
    """Calculate technical indicators"""

    @staticmethod
    def sma(data: pd.Series, period: int) -> pd.Series:
        return data.rolling(window=period).mean()

    @staticmethod
    def ema(data: pd.Series, period: int) -> pd.Series:
        return data.ewm(span=period, adjust=False).mean()

    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> pd.Series:
        """RSI using Wilder's smoothing (EMA with alpha = 1/period)"""
        delta = data.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.fillna(100)
        return rsi

    @staticmethod
    def macd(data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        ema_fast = data.ewm(span=fast, adjust=False).mean()
        ema_slow = data.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def bollinger_bands(data: pd.Series, period: int = 20, std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        sma = data.rolling(window=period).mean()
        std = data.rolling(window=period).std()
        upper = sma + (std * std_dev)
        lower = sma - (std * std_dev)
        return upper, sma, lower

    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()

    @staticmethod
    def stochastic(high: pd.Series, low: pd.Series, close: pd.Series,
                   k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
        lowest_low = low.rolling(window=k_period).min()
        highest_high = high.rolling(window=k_period).max()
        k = 100 * (close - lowest_low) / (highest_high - lowest_low)
        d = k.rolling(window=d_period).mean()
        return k, d

    @staticmethod
    def williams_r(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Williams %R: oscillates between -100 (oversold) and 0 (overbought)."""
        highest_high = high.rolling(window=period).max()
        lowest_low = low.rolling(window=period).min()
        rng = (highest_high - lowest_low).replace(0, np.nan)
        return -100 * (highest_high - close) / rng

    @staticmethod
    def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        direction = np.where(close > close.shift(1), 1, np.where(close < close.shift(1), -1, 0))
        return (volume * direction).cumsum()

    @staticmethod
    def adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        plus_dm = high.diff()
        minus_dm = -low.diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0

        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = tr.rolling(window=period).mean()
        plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)

        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
        adx = dx.rolling(window=period).mean()
        return adx

    @staticmethod
    def parabolic_sar(high: pd.Series, low: pd.Series, close: pd.Series,
                      af_start: float = 0.02, af_step: float = 0.02, af_max: float = 0.2) -> pd.Series:
        """Calculate Parabolic SAR indicator"""
        length = len(close)
        sar = pd.Series(index=close.index, dtype=float)
        trend = pd.Series(index=close.index, dtype=int)

        sar.iloc[0] = low.iloc[0]
        trend.iloc[0] = 1
        ep = high.iloc[0]
        af = af_start

        for i in range(1, length):
            if trend.iloc[i - 1] == 1:
                sar.iloc[i] = sar.iloc[i - 1] + af * (ep - sar.iloc[i - 1])
                sar.iloc[i] = min(sar.iloc[i], low.iloc[i - 1], low.iloc[i - 2] if i > 1 else low.iloc[i - 1])

                if low.iloc[i] < sar.iloc[i]:
                    trend.iloc[i] = -1
                    sar.iloc[i] = ep
                    ep = low.iloc[i]
                    af = af_start
                else:
                    trend.iloc[i] = 1
                    if high.iloc[i] > ep:
                        ep = high.iloc[i]
                        af = min(af + af_step, af_max)
            else:
                sar.iloc[i] = sar.iloc[i - 1] + af * (ep - sar.iloc[i - 1])
                sar.iloc[i] = max(sar.iloc[i], high.iloc[i - 1], high.iloc[i - 2] if i > 1 else high.iloc[i - 1])

                if high.iloc[i] > sar.iloc[i]:
                    trend.iloc[i] = 1
                    sar.iloc[i] = ep
                    ep = high.iloc[i]
                    af = af_start
                else:
                    trend.iloc[i] = -1
                    if low.iloc[i] < ep:
                        ep = low.iloc[i]
                        af = min(af + af_step, af_max)

        return sar


# ============================================================================
# TLT SCANNER ENGINE
# ============================================================================

class TLTEngine:
    """
    TLT (Trend-Liquidity-Timing) Scanner Engine
    Calculates: MFI, RSI, LR (MFI/RSI ratio), Mansfield RS, CMF
    Signal Tiers: LEADER, SURGE, SPRING, DANGER, OVERSOLD, NEUTRAL

    Modes:
    - "high_conviction": Fewer signals, highest win rates (91.9% OVERSOLD, 82.2% SPRING)
    - "balanced": More signals, strong win rates (83.6% OVERSOLD, 75% SPRING)
    """

    def __init__(self, benchmark_data: pd.DataFrame = None, mode: str = "high_conviction"):
        self.benchmark_data = benchmark_data
        self.ti = TechnicalIndicators()
        self.mode = mode

    @staticmethod
    def calculate_mfi(high: pd.Series, low: pd.Series, close: pd.Series,
                      volume: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Money Flow Index (MFI)"""
        tp = (high + low + close) / 3
        rmf = tp * volume

        tp_diff = tp.diff()
        mf_positive = np.where(tp_diff > 0, rmf, 0)
        mf_negative = np.where(tp_diff < 0, rmf, 0)

        mf_pos_sum = pd.Series(mf_positive, index=close.index).rolling(window=period).sum()
        mf_neg_sum = pd.Series(mf_negative, index=close.index).rolling(window=period).sum()

        mfr = mf_pos_sum / (mf_neg_sum + 1e-10)
        mfi = 100 - (100 / (1 + mfr))
        return mfi

    @staticmethod
    def calculate_cmf(high: pd.Series, low: pd.Series, close: pd.Series,
                      volume: pd.Series, period: int = 20) -> pd.Series:
        """Calculate Chaikin Money Flow (CMF)"""
        hl_range = high - low
        hl_range = hl_range.replace(0, 1e-10)
        mfm = ((close - low) - (high - close)) / hl_range
        mfv = mfm * volume
        cmf = mfv.rolling(window=period).sum() / volume.rolling(window=period).sum()
        return cmf

    def calculate_mansfield_rs(self, stock_close: pd.Series,
                               lookback: int = 252) -> pd.Series:
        """Mansfield Relative Strength vs benchmark"""
        if self.benchmark_data is None or self.benchmark_data.empty:
            return pd.Series(0, index=stock_close.index)

        if "Close" in self.benchmark_data.columns:
            benchmark_close = self.benchmark_data["Close"]
        elif "close" in self.benchmark_data.columns:
            benchmark_close = self.benchmark_data["close"]
        else:
            return pd.Series(0, index=stock_close.index)

        common_idx = stock_close.index.intersection(benchmark_close.index)

        if len(common_idx) < lookback:
            lookback = max(50, len(common_idx) // 2)

        stock_aligned = stock_close.loc[common_idx]
        bench_aligned = benchmark_close.loc[common_idx]

        rs_ratio = (stock_aligned / bench_aligned) * 100
        rs_sma = rs_ratio.rolling(window=min(lookback, len(rs_ratio))).mean()
        mrs = ((rs_ratio / rs_sma) - 1) * 10

        result = pd.Series(index=stock_close.index, dtype=float)
        result.loc[common_idx] = mrs
        return result.fillna(0)

    def analyze_stock(self, df: pd.DataFrame) -> Optional[Dict]:
        """Analyze a single stock and return TLT metrics and signal tier"""
        if df is None or len(df) < 60:
            return None

        try:
            close = df["Close"]
            high = df["High"]
            low = df["Low"]
            volume = df["Volume"]
            current_price = close.iloc[-1]

            mfi = self.calculate_mfi(high, low, close, volume, 14)
            rsi = self.ti.rsi(close, 14)
            cmf = self.calculate_cmf(high, low, close, volume, 20)
            mrs = self.calculate_mansfield_rs(close)

            ma20 = self.ti.sma(close, 20)
            ma50 = self.ti.sma(close, 50)
            ma200 = self.ti.sma(close, 200)

            atr = self.ti.atr(high, low, close, 14)

            current_mfi = mfi.iloc[-1] if not pd.isna(mfi.iloc[-1]) else 50
            current_rsi = rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else 50
            current_cmf = cmf.iloc[-1] if not pd.isna(cmf.iloc[-1]) else 0
            current_mrs = mrs.iloc[-1] if not pd.isna(mrs.iloc[-1]) else 0
            current_ma20 = ma20.iloc[-1] if not pd.isna(ma20.iloc[-1]) else current_price
            current_ma50 = ma50.iloc[-1] if not pd.isna(ma50.iloc[-1]) else current_price
            current_ma200 = ma200.iloc[-1] if not pd.isna(ma200.iloc[-1]) else current_price
            current_atr = atr.iloc[-1] if not pd.isna(atr.iloc[-1]) else current_price * 0.02

            lr_ratio = current_mfi / (current_rsi + 1e-10)

            cmf_5d_ago = cmf.iloc[-6] if len(cmf) >= 6 and not pd.isna(cmf.iloc[-6]) else current_cmf
            cmf_rising = current_cmf > cmf_5d_ago

            mrs_prev = mrs.iloc[-2] if len(mrs) >= 2 and not pd.isna(mrs.iloc[-2]) else current_mrs
            mrs_rising = current_mrs > mrs_prev

            lr_prev_mfi = mfi.iloc[-2] if len(mfi) >= 2 and not pd.isna(mfi.iloc[-2]) else current_mfi
            lr_prev_rsi = rsi.iloc[-2] if len(rsi) >= 2 and not pd.isna(rsi.iloc[-2]) else current_rsi
            lr_prev = lr_prev_mfi / (lr_prev_rsi + 1e-10)
            lr_rising = lr_ratio > lr_prev

            above_ma20 = current_price > current_ma20
            above_ma50 = current_price > current_ma50
            above_ma200 = current_price > current_ma200

            avg_vol_20 = volume.iloc[-20:].mean()
            rel_vol = volume.iloc[-1] / avg_vol_20 if avg_vol_20 > 0 else 1
            vol_surge = rel_vol > 1.5

            if len(close) >= 252:
                hi_52 = close.rolling(252).max().iloc[-1]
                lo_52 = close.rolling(252).min().iloc[-1]
            else:
                hi_52 = close.max()
                lo_52 = close.min()
            vs_52hi = ((current_price / hi_52) - 1) * 100 if hi_52 > 0 else 0
            pct_range = ((current_price - lo_52) / (hi_52 - lo_52) * 100) if hi_52 != lo_52 else 50

            vs_ma50 = ((current_price / current_ma50) - 1) * 100

            signal_tier, tier_emoji = self._classify_signal(
                lr_ratio, current_cmf, cmf_rising, current_mrs, mrs_rising,
                above_ma20, above_ma50, above_ma200, rsi=current_rsi,
                vs_ma50=vs_ma50, mode=self.mode
            )

            composite_score = self._calculate_composite_score(
                lr_ratio, current_cmf, cmf_rising, current_mrs, mrs_rising,
                above_ma20, above_ma50, above_ma200
            )

            stop_tight = round(current_price - 1.5 * current_atr, 2)
            stop_wide = round(current_price - 2.5 * current_atr, 2)
            target_1 = round(current_price + 2.0 * current_atr, 2)
            target_2 = round(current_price + 4.0 * current_atr, 2)

            return {
                "MFI": round(current_mfi, 1),
                "RSI": round(current_rsi, 1),
                "LR_Ratio": round(lr_ratio, 3),
                "LR_Rising": lr_rising,
                "CMF": round(current_cmf, 4),
                "CMF_Rising": cmf_rising,
                "MRS": round(current_mrs, 3),
                "MRS_Rising": mrs_rising,
                "RelVol": round(rel_vol, 2),
                "VolSurge": vol_surge,
                "Above_MA20": above_ma20,
                "Above_MA50": above_ma50,
                "Above_MA200": above_ma200,
                "vs_MA20": round(((current_price / current_ma20) - 1) * 100, 1),
                "vs_MA50": round(((current_price / current_ma50) - 1) * 100, 1),
                "vs_MA200": round(((current_price / current_ma200) - 1) * 100, 1),
                "vs_52wHigh": round(vs_52hi, 1),
                "PctRange52w": round(pct_range, 1),
                "Signal_Tier": signal_tier,
                "Tier_Emoji": tier_emoji,
                "Composite_Score": composite_score,
                "Price": round(current_price, 2),
                "ATR": round(current_atr, 2),
                "StopTight": stop_tight,
                "StopWide": stop_wide,
                "Target1": target_1,
                "Target2": target_2,
                "MA20": round(current_ma20, 2),
                "MA50": round(current_ma50, 2),
                "MA200": round(current_ma200, 2),
            }
        except Exception:
            return None

    def _classify_signal(self, lr_ratio, cmf, cmf_rising, mrs, mrs_rising,
                         above_ma20, above_ma50, above_ma200, rsi: float = 50,
                         vs_ma50: float = 0, mode: str = "high_conviction") -> Tuple[str, str]:
        """
        Classify into signal tiers based on OPTIMIZED TLT criteria.
        OPTIMIZED via Full S&P 500 Backtest (435 stocks, 469,611 signals, 5 years)
        """
        if lr_ratio > 1.5 and not mrs_rising:
            return "DANGER", "🔴"

        if mode == "high_conviction":
            if lr_ratio >= 1.5 and cmf > 0.1 and mrs_rising and rsi < 30:
                return "OVERSOLD", "🟡"
            if lr_ratio >= 1.4 and cmf > 0.03 and mrs_rising and rsi >= 60 and mrs >= 1.0:
                return "SURGE", "🔵"
            if lr_ratio >= 1.2 and cmf > 0.05 and mrs >= 1.5 and mrs_rising:
                return "LEADER", "🚀"
            if lr_ratio > 1.2 and cmf > 0.05 and not above_ma50 and vs_ma50 < -15:
                return "SPRING", "🌱"
        else:
            if lr_ratio >= 1.15 and cmf > 0.05 and mrs_rising and rsi < 30:
                return "OVERSOLD", "🟡"
            if lr_ratio >= 1.3 and cmf > 0.05 and mrs_rising and rsi >= 50 and mrs >= 0.5:
                return "SURGE", "🔵"
            if lr_ratio >= 1.1 and cmf > 0.08 and mrs >= 1.0 and mrs_rising:
                return "LEADER", "🚀"
            if lr_ratio > 1.0 and cmf > 0.03 and not above_ma50 and vs_ma50 < -10:
                return "SPRING", "🌱"

        return "NEUTRAL", "⚪"

    def _calculate_composite_score(self, lr_ratio, cmf, cmf_rising, mrs, mrs_rising,
                                   above_ma20, above_ma50, above_ma200) -> int:
        """Calculate composite score 0-100"""
        score = 0
        score += min(lr_ratio * 20, 30)
        score += min(cmf * 100, 25)
        if mrs >= 0:
            score += 20
        elif mrs_rising:
            score += 10
        if mrs_rising:
            score += 10
        if above_ma200:
            score += 15
        elif above_ma50:
            score += 10
        elif above_ma20:
            score += 5
        return max(0, min(100, round(score)))


# ============================================================================
# HEADLESS PRICE FETCH (used by tlt_signal_tracker.py)
# ============================================================================

_PERIOD_DAYS = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "2y": 730, "5y": 1825}


def _fetch_fmp(symbol: str, period: str, session: requests.Session) -> Optional[pd.DataFrame]:
    if not FMP_API_KEY:
        return None
    try:
        url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?apikey={FMP_API_KEY}"
        response = session.get(url, timeout=15)
        data = response.json()
        if "historical" not in data:
            return None
        df = pd.DataFrame(data["historical"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").set_index("date")
        cutoff = datetime.now() - timedelta(days=_PERIOD_DAYS.get(period, 365))
        df = df[df.index >= cutoff]
        df = df.rename(columns={"open": "Open", "high": "High", "low": "Low",
                                 "close": "Close", "volume": "Volume"})
        return df[["Open", "High", "Low", "Close", "Volume"]]
    except Exception:
        return None


def _fetch_yfinance(symbol: str, period: str) -> Optional[pd.DataFrame]:
    try:
        df = yf.Ticker(symbol).history(period=period)
        if df.empty:
            return None
        return df[["Open", "High", "Low", "Close", "Volume"]]
    except Exception:
        return None


def fetch_history(symbol: str, period: str = "1y",
                  session: Optional[requests.Session] = None) -> Optional[pd.DataFrame]:
    """FMP primary, yfinance fallback. Returns DataFrame indexed by date."""
    sess = session or requests.Session()
    df = _fetch_fmp(symbol, period, sess)
    if df is not None and not df.empty:
        return df
    return _fetch_yfinance(symbol, period)
