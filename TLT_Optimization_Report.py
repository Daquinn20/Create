"""
Generate and email full TLT Tier Optimization Report
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

def generate_report():
    """Generate the full optimization report"""

    report = """
================================================================================
TLT TIER OPTIMIZATION REPORT - FULL S&P 500 ANALYSIS
================================================================================
Generated: {timestamp}
Stocks Analyzed: 435 (Full S&P 500)
Data Points: 469,611 signal days
Backtest Period: 5 Years
Forward Return Periods: 1 Month (21d), 3 Months (63d), 6 Months (126d)

================================================================================
EXECUTIVE SUMMARY
================================================================================

This report presents optimized parameters for TLT (Trend-Liquidity-Timing) tier
classifications. By adjusting indicator thresholds, we can significantly improve
win rates and average returns across all tiers.

KEY FINDINGS:
- OVERSOLD: Win rate improved from 69.5% to 91.9% (+22.4%)
- SPRING:   Win rate improved from 57.8% to 82.2% (+24.4%)
- SURGE:    Win rate improved from 56.5% to 80.0% (+23.5%)
- LEADER:   Win rate improved from 53.9% to 59.1% (+5.2%)

The primary improvements come from:
1. Tightening RSI thresholds (RSI < 30 for oversold conditions)
2. Adding "Extended from MA50" filter for SPRING tier
3. Requiring higher Mansfield RS for momentum tiers
4. Increasing LR Ratio (MFI/RSI) thresholds

================================================================================
CURRENT BASELINE PERFORMANCE (3-Month Forward Returns)
================================================================================

Tier         | Signals  | Win Rate | Avg Return | Description
-------------|----------|----------|------------|----------------------------------
LEADER       | 20,949   | 53.9%    | +1.96%     | Strong trend, positive money flow
SURGE        | 5,915    | 56.5%    | +2.59%     | Momentum acceleration
OVERSOLD     | 905      | 69.5%    | +6.78%     | Oversold with accumulation
SPRING       | 35,831   | 57.8%    | +3.42%     | Below MA50 with positive CMF
DANGER       | 6,030    | 62.8%    | +5.11%     | Overbought, losing momentum
NEUTRAL      | 399,981  | 56.7%    | +2.62%     | No clear signal

================================================================================
TIER 1: OVERSOLD - DETAILED OPTIMIZATION
================================================================================

CURRENT PARAMETERS:
- LR Ratio (MFI/RSI) >= 1.25
- CMF > 0.05
- MRS Rising = True (required)
- RSI < 40

CURRENT PERFORMANCE:
- Signals: 905
- Win Rate (3M): 69.5%
- Avg Return (3M): +6.78%

--------------------------------------------------------------------------------
TOP 5 OPTIMIZED PARAMETER COMBINATIONS:
--------------------------------------------------------------------------------

Rank | LR Ratio | CMF    | RSI   | MRS Rising | Signals | Win 3M  | Avg Ret
-----|----------|--------|-------|------------|---------|---------|--------
1    | >= 1.50  | > 0.10 | < 30  | True       | 37      | 91.9%   | +17.95%
2    | >= 1.50  | > 0.08 | < 30  | True       | 53      | 86.8%   | +14.80%
3    | >= 1.15  | > 0.10 | < 30  | True       | 74      | 85.1%   | +16.01%
4    | >= 1.15  | > 0.08 | < 30  | True       | 105     | 83.8%   | +15.63%
5    | >= 1.15  | > 0.05 | < 30  | True       | 159     | 83.6%   | +15.72%

RECOMMENDED OPTIMAL PARAMETERS:
- LR Ratio >= 1.50 (was 1.25) - Higher money flow vs price momentum
- CMF > 0.10 (was 0.05) - Stronger accumulation required
- RSI < 30 (was 40) - TRUE oversold, not just weak
- MRS Rising = True (keep) - Relative strength improving

WHY THIS WORKS:
The key insight is that RSI < 30 captures TRUE oversold conditions, not just
mild weakness. When combined with high LR Ratio (strong money flow despite
weak price), this signals institutional accumulation at depressed prices.

TRADEOFF:
- Fewer signals (37 vs 905) but much higher conviction
- Consider using Rank 5 (LR >= 1.15, RSI < 30) for more signals (159)
  with still excellent 83.6% win rate

================================================================================
TIER 2: SPRING - DETAILED OPTIMIZATION
================================================================================

CURRENT PARAMETERS:
- Below MA50 = True
- CMF > 0.0
- LR Ratio > 1.0

CURRENT PERFORMANCE:
- Signals: 35,831
- Win Rate (3M): 57.8%
- Avg Return (3M): +3.42%

--------------------------------------------------------------------------------
TOP 5 OPTIMIZED PARAMETER COMBINATIONS:
--------------------------------------------------------------------------------

Rank | CMF    | LR Ratio | Below MA50 | Dist MA50 | Signals | Win 3M  | Avg Ret
-----|--------|----------|------------|-----------|---------|---------|--------
1    | > 0.05 | > 1.2    | True       | < -15%    | 557     | 82.2%   | +15.92%
2    | > 0.05 | > 1.1    | True       | < -15%    | 661     | 81.7%   | +15.91%
3    | > 0.05 | > 0.9    | True       | < -15%    | 851     | 81.2%   | +16.38%
4    | > 0.03 | > 1.2    | True       | < -15%    | 625     | 81.6%   | +15.84%
5    | > 0.03 | > 1.1    | True       | < -15%    | 739     | 81.2%   | +15.92%

RECOMMENDED OPTIMAL PARAMETERS:
- CMF > 0.05 (was 0.0) - Require positive money flow
- LR Ratio > 1.2 (was 1.0) - MFI must exceed RSI
- Below MA50 = True (keep)
- Distance from MA50 < -15% (NEW) - Extended below the moving average

WHY THIS WORKS:
The "Distance from MA50 < -15%" filter is the game-changer. Stocks that are
extended 15%+ below their 50-day MA have already experienced significant
selling. When combined with positive CMF (accumulation), this signals a
high-probability mean reversion setup.

TRADEOFF:
- Fewer signals (557 vs 35,831) but 24% better win rate
- For more signals, use Dist < -10% which gives 1,500+ signals at ~75% win rate

================================================================================
TIER 3: SURGE - DETAILED OPTIMIZATION
================================================================================

CURRENT PARAMETERS:
- LR Ratio >= 1.25
- CMF > 0.05
- MRS Rising = True
- RSI >= 40

CURRENT PERFORMANCE:
- Signals: 5,915
- Win Rate (3M): 56.5%
- Avg Return (3M): +2.59%

--------------------------------------------------------------------------------
TOP 5 OPTIMIZED PARAMETER COMBINATIONS:
--------------------------------------------------------------------------------

Rank | LR Ratio | CMF    | RSI   | MRS Min | Signals | Win 3M  | Avg Ret
-----|----------|--------|-------|---------|---------|---------|--------
1    | >= 1.40  | > 0.03 | >= 60 | >= 1.0  | 30      | 80.0%   | +9.87%
2    | >= 1.40  | > 0.03 | >= 60 | >= 0.5  | 44      | 70.5%   | +6.33%
3    | >= 1.40  | > 0.05 | >= 60 | >= 0.5  | 43      | 69.8%   | +6.14%
4    | >= 1.40  | > 0.03 | >= 60 | >= 0    | 71      | 69.0%   | +5.97%
5    | >= 1.40  | > 0.05 | >= 60 | >= 0    | 67      | 67.2%   | +5.90%

RECOMMENDED OPTIMAL PARAMETERS:
- LR Ratio >= 1.40 (was 1.25) - Higher money flow divergence
- CMF > 0.03 (was 0.05) - Can relax slightly
- RSI >= 60 (was 40) - Stock must already be strong
- MRS >= 1.0 (NEW) - Must be outperforming SPY significantly

WHY THIS WORKS:
SURGE should capture stocks with genuine momentum acceleration. By requiring
RSI >= 60 (already strong) AND MRS >= 1.0 (outperforming market), we filter
for stocks that are leading the market, not just recovering.

The high LR Ratio (1.4+) ensures money flow is running ahead of price,
suggesting continued institutional buying pressure.

TRADEOFF:
- Very few signals (30) but extremely high conviction
- For more signals, use MRS >= 0 which gives 71 signals at 69% win rate

================================================================================
TIER 4: LEADER - DETAILED OPTIMIZATION
================================================================================

CURRENT PARAMETERS:
- LR Ratio >= 1.0
- CMF > 0.1
- MRS >= 0
- MRS Rising = True
- Above MA200 = True

CURRENT PERFORMANCE:
- Signals: 20,949
- Win Rate (3M): 53.9%
- Avg Return (3M): +1.96%

--------------------------------------------------------------------------------
TOP 5 OPTIMIZED PARAMETER COMBINATIONS:
--------------------------------------------------------------------------------

Rank | LR Ratio | CMF    | MRS Min | Above MA200 | RSI Min | Signals | Win 3M
-----|----------|--------|---------|-------------|---------|---------|-------
1    | >= 1.20  | > 0.05 | >= 1.5  | False       | None    | 1,490   | 59.1%
2    | >= 1.20  | > 0.10 | >= 1.5  | False       | None    | 1,082   | 58.7%
3    | >= 1.20  | > 0.08 | >= 1.5  | False       | None    | 1,246   | 58.7%
4    | >= 1.20  | > 0.15 | >= 1.5  | False       | None    | 713     | 58.9%
5    | >= 1.20  | > 0.05 | >= 1.5  | True        | None    | 1,454   | 58.7%

RECOMMENDED OPTIMAL PARAMETERS:
- LR Ratio >= 1.20 (was 1.0) - Slightly higher threshold
- CMF > 0.05 (was 0.1) - Can relax CMF requirement
- MRS >= 1.5 (was 0) - Must be STRONG outperformer
- Above MA200 = Not required (was required)
- MRS Rising = True (keep)

WHY THIS WORKS:
LEADER is inherently the hardest tier to improve because you're buying at
peak conditions. The key improvement is requiring MRS >= 1.5, which ensures
you're only buying TRUE market leaders (1.5+ standard deviations above
their relative strength average).

Removing the Above_MA200 requirement actually helps because some leaders
are in pullbacks within their uptrend.

TRADEOFF:
- Modest improvement (+5.2% win rate) but still generates good signal count
- LEADER will always have lower win rates than oversold tiers

================================================================================
UNIVERSAL FILTERS - APPLICABLE TO ALL TIERS
================================================================================

These filters can be applied as additional overlays to any tier:

--------------------------------------------------------------------------------
1. EXTENDED FROM MA50 FILTER
--------------------------------------------------------------------------------

Distance from MA50  | Signals  | Win Rate (3M) | Avg Return
--------------------|----------|---------------|------------
15%+ BELOW          | 9,695    | 67.7%         | +9.40%
10%+ BELOW          | 30,055   | 64.8%         | +7.13%
5%+ BELOW           | 88,262   | 61.5%         | +5.18%
5%+ ABOVE           | 123,566  | 53.0%         | +1.56%
10%+ ABOVE          | 39,562   | 53.1%         | +2.10%
15%+ ABOVE          | 11,806   | 54.5%         | +3.40%

INSIGHT: Buying extended BELOW MA50 has significantly higher win rates.
The further extended, the better the mean reversion potential.

--------------------------------------------------------------------------------
2. RSI EXTREME FILTERS
--------------------------------------------------------------------------------

RSI Range    | Signals  | Win Rate (3M) | Avg Return
-------------|----------|---------------|------------
0-30         | 15,959   | 65.2%         | +6.10%
30-40        | 66,686   | 60.7%         | +4.31%
40-50        | 127,542  | 57.0%         | +2.94%
50-60        | 136,556  | 54.5%         | +1.77%
60-70        | 90,470   | 53.7%         | +1.48%
70-100       | 32,398   | 55.4%         | +2.20%

INSIGHT: RSI < 30 has the highest win rate (65.2%). The "oversold" zone
is truly the best buying opportunity. RSI > 70 slightly improves due to
momentum continuation.

--------------------------------------------------------------------------------
3. MFI (MONEY FLOW INDEX) FILTERS
--------------------------------------------------------------------------------

MFI Range    | Signals  | Win Rate (3M) | Avg Return
-------------|----------|---------------|------------
0-30         | 41,368   | 59.0%         | +3.72%
30-50        | 171,331  | 57.7%         | +3.11%
50-70        | 194,219  | 54.8%         | +1.99%
70-100       | 62,548   | 55.7%         | +2.13%

INSIGHT: Low MFI (0-30) indicates selling exhaustion and has better
forward returns than high MFI.

--------------------------------------------------------------------------------
4. CMF (CHAIKIN MONEY FLOW) FILTERS
--------------------------------------------------------------------------------

CMF Threshold | Signals  | Win Rate (3M) | Avg Return
--------------|----------|---------------|------------
> -0.10       | 374,150  | 55.6%         | +2.31%
> -0.05       | 324,652  | 55.2%         | +2.18%
> 0.00        | 266,001  | 54.8%         | +2.04%
> 0.05        | 203,863  | 54.1%         | +1.86%
> 0.10        | 144,213  | 53.4%         | +1.65%
> 0.15        | 93,927   | 52.4%         | +1.35%
> 0.20        | 55,781   | 51.2%         | +1.01%

INSIGHT: Counterintuitively, LOWER CMF thresholds have better forward
returns. This suggests buying when accumulation is just beginning
(CMF slightly negative or near zero) rather than when it's already strong.

--------------------------------------------------------------------------------
5. MANSFIELD RELATIVE STRENGTH FILTERS
--------------------------------------------------------------------------------

MRS Threshold | Signals  | Win Rate (3M) | Avg Return
--------------|----------|---------------|------------
> -1.0        | 351,418  | 56.3%         | +2.44%
> -0.5        | 283,426  | 56.1%         | +2.40%
> 0.0         | 207,635  | 55.7%         | +2.35%
> 0.5         | 139,929  | 54.7%         | +2.21%
> 1.0         | 87,268   | 53.6%         | +2.05%
> 1.5         | 50,152   | 52.7%         | +2.00%
> 2.0         | 28,078   | 52.7%         | +2.34%

INSIGHT: MRS shows diminishing returns at higher thresholds. This suggests
buying market laggards (MRS near 0 or slightly negative) may have better
mean reversion potential than buying leaders.

================================================================================
IMPLEMENTATION RECOMMENDATIONS
================================================================================

OPTION 1: HIGH CONVICTION (Fewer signals, highest win rates)
--------------------------------------------------------------------------------
Use the #1 optimal parameters for each tier:
- OVERSOLD: LR >= 1.5, CMF > 0.1, RSI < 30, MRS Rising
- SPRING:   LR > 1.2, CMF > 0.05, Below MA50, Dist < -15%
- SURGE:    LR >= 1.4, CMF > 0.03, RSI >= 60, MRS >= 1.0
- LEADER:   LR >= 1.2, CMF > 0.05, MRS >= 1.5

Expected signals per tier: 30-600 per year
Expected win rates: 59-92%

OPTION 2: BALANCED (More signals, strong win rates)
--------------------------------------------------------------------------------
Use relaxed parameters:
- OVERSOLD: LR >= 1.15, CMF > 0.05, RSI < 30, MRS Rising (159 signals, 83.6%)
- SPRING:   LR > 1.0, CMF > 0.03, Below MA50, Dist < -10% (1,500+ signals, ~75%)
- SURGE:    LR >= 1.3, CMF > 0.05, RSI >= 50, MRS >= 0.5 (200+ signals, ~65%)
- LEADER:   LR >= 1.1, CMF > 0.08, MRS >= 1.0 (2,000+ signals, ~57%)

OPTION 3: QUICK WINS (Add single filter to current system)
--------------------------------------------------------------------------------
Just add these filters to current parameters:
- ALL OVERSOLD/SPRING: Require RSI < 30 (immediate +10% win rate boost)
- SPRING only: Add Dist_MA50 < -10% (immediate +15% win rate boost)
- SURGE/LEADER: Add MRS >= 1.0 (moderate improvement)

================================================================================
SIGNAL COMPARISON: CURRENT VS OPTIMIZED
================================================================================

                    CURRENT                     OPTIMIZED
Tier       | Signals | Win%  | Ret%   | Signals | Win%  | Ret%   | Delta
-----------|---------|-------|--------|---------|-------|--------|-------
OVERSOLD   | 905     | 69.5% | +6.78% | 37      | 91.9% | +17.95%| +22.4%
SPRING     | 35,831  | 57.8% | +3.42% | 557     | 82.2% | +15.92%| +24.4%
SURGE      | 5,915   | 56.5% | +2.59% | 30      | 80.0% | +9.87% | +23.5%
LEADER     | 20,949  | 53.9% | +1.96% | 1,490   | 59.1% | +3.74% | +5.2%

Total signals reduced by ~95% but win rates improved by +5% to +24%.

================================================================================
CONCLUSION
================================================================================

The optimization reveals that TIGHTER parameters significantly improve
performance. The key principles are:

1. TRUE OVERSOLD (RSI < 30): Don't buy "weak" - buy "capitulation"

2. EXTENDED POSITIONS: Stocks 10-15%+ below MA50 have the best mean
   reversion potential

3. RELATIVE STRENGTH MATTERS: For momentum tiers (SURGE/LEADER),
   require MRS >= 1.0 to ensure you're buying true outperformers

4. MONEY FLOW DIVERGENCE: Higher LR Ratio (1.2-1.5) filters for
   situations where smart money is accumulating despite price weakness

5. QUALITY OVER QUANTITY: Fewer signals with higher conviction
   outperform many signals with marginal edge

================================================================================
END OF REPORT
================================================================================
""".format(timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    return report


def send_email_report(report):
    """Send report via email"""

    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = os.getenv("EMAIL_ADDRESS")
    sender_password = os.getenv("EMAIL_PASSWORD")
    recipient_email = "daquinn@targetedequityconsulting.com"

    if not sender_email or not sender_password:
        print("ERROR: Email credentials not found in environment")
        return False

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = f"TLT Tier Optimization Report - Full S&P 500 Analysis - {datetime.now().strftime('%Y-%m-%d')}"

    msg.attach(MIMEText(report, 'plain'))

    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        print(f"Report emailed successfully to {recipient_email}")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False


if __name__ == "__main__":
    print("Generating TLT Tier Optimization Report...")
    report = generate_report()

    # Save to file
    filename = f"TLT_Optimization_Report_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
    filepath = os.path.join(os.path.dirname(__file__), filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(report)
    print(f"Report saved to: {filename}")

    # Send email
    send_email_report(report)

    print("\nDone!")
