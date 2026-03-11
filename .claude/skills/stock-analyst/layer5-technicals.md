# Layer 5: Technical Signal Analysis

> Buffett doesn't use charts, but knowing when the market agrees with
> your thesis helps with entry timing and risk management.

## Purpose

Technical analysis is the LAST layer, not the first. Use it to:
1. Optimize entry/exit timing after Layers 1-4 confirm the thesis
2. Identify support/resistance for position sizing
3. Spot momentum divergences as early warning signals
4. Set stop-loss and profit-taking levels

## Core Technical Indicators

### Trend Indicators

**Moving Averages:**
| MA | Timeframe | Signal |
|----|-----------|--------|
| 20-day SMA/EMA | Short-term | Immediate trend direction |
| 50-day SMA | Medium-term | Swing trade reference |
| 200-day SMA | Long-term | Bull/bear market definition |

**Key Signals:**
- Golden Cross: 50-day crosses ABOVE 200-day → bullish long-term
- Death Cross: 50-day crosses BELOW 200-day → bearish long-term
- Price above all MAs: Strong uptrend
- Price below all MAs: Strong downtrend
- MAs converging: Trend change brewing

**For long-term investors (Buffett style)**:
The 200-day MA is the most important. Buying quality stocks when they
dip to or below the 200-day MA has historically been rewarding.

### Momentum Indicators

**RSI (Relative Strength Index, 14-period):**
| RSI Level | Interpretation | Action Consideration |
|-----------|---------------|---------------------|
| > 70 | Overbought | Caution for new buys, not necessarily sell |
| 50-70 | Bullish momentum | Trend is healthy |
| 30-50 | Bearish momentum | Weakening, watch closely |
| < 30 | Oversold | Potential bounce, look for confirmation |

**Bullish RSI Divergence**: Price makes lower low, RSI makes higher low
→ momentum improving, potential reversal up

**Bearish RSI Divergence**: Price makes higher high, RSI makes lower high
→ momentum weakening, potential reversal down

**MACD (12, 26, 9):**
- MACD line crosses above signal line → bullish
- MACD line crosses below signal line → bearish
- MACD histogram growing → trend strengthening
- MACD divergence from price → potential reversal

### Volume Analysis

Volume confirms or denies price moves.

| Price Action | Volume | Interpretation |
|-------------|--------|----------------|
| Up | High | Strong buying conviction ✅ |
| Up | Low | Weak rally, may not sustain ⚠️ |
| Down | High | Strong selling pressure ⚠️ |
| Down | Low | Lack of selling interest, may bounce ✅ |
| Breakout | High | Confirmed breakout ✅ |
| Breakout | Low | Likely false breakout ⚠️ |

**On-Balance Volume (OBV)**:
- OBV trending up while price is flat → accumulation (bullish)
- OBV trending down while price is flat → distribution (bearish)

### Support & Resistance

**How to Identify:**
- Previous highs/lows
- Round numbers ($100, $200, $500)
- Moving average levels (50-day, 200-day)
- Volume profile peaks (where most trading occurred)
- Gap levels (unfilled gaps act as magnets)

**Trading Implications:**
- Buy near support with tight stop below
- Take profits near resistance
- Breakout above resistance with volume → new uptrend
- Breakdown below support with volume → new downtrend

### Chart Patterns (Simplified)

**Bullish Patterns:**
- Double bottom (W shape): Tested support twice, held → buy signal
- Cup & handle: Rounded bottom with small pullback → continuation
- Ascending triangle: Higher lows + flat resistance → breakout likely up
- Bull flag: Strong move up + small pullback → continuation

**Bearish Patterns:**
- Double top (M shape): Failed to break resistance twice → sell signal
- Head & shoulders: Three peaks, middle highest → trend reversal
- Descending triangle: Flat support + lower highs → breakdown likely
- Bear flag: Strong move down + small bounce → continuation down

## Combining Technicals with Fundamentals

**The Sweet Spot Matrix:**

| Fundamentals (L1-4) | Technicals (L5) | Action |
|---------------------|-----------------|--------|
| 🟢 Strong | 🟢 Bullish | Strong Buy — full position |
| 🟢 Strong | 🟡 Neutral | Buy — start building position |
| 🟢 Strong | 🔴 Bearish | Watch — great company at bad time, wait for reversal |
| 🟡 Mixed | 🟢 Bullish | Small position — momentum may not last |
| 🔴 Weak | 🟢 Bullish | Avoid — momentum chasing, no foundation |
| 🔴 Weak | 🔴 Bearish | Strong Avoid — nothing going for it |

## Position Sizing & Risk Management

**Based on conviction level:**
| Conviction | Position Size | Stop Loss |
|-----------|--------------|-----------|
| High (L1-4 all 🟢) | 5-10% of portfolio | 15-20% below entry |
| Medium (Mixed signals) | 2-5% of portfolio | 10-15% below entry |
| Speculative | 1-2% of portfolio | 7-10% below entry |

**Never risk more than 2% of total portfolio on a single trade.**

## Scoring Rubric

🟢 **Bullish Technical Setup** (3 points): Price above key MAs,
   bullish pattern, RSI healthy (not overbought), strong volume

🟡 **Neutral/Transitioning** (2 points): Mixed signals, consolidation,
   no clear trend

🔴 **Bearish Technical Setup** (1 point): Price below key MAs,
   bearish pattern, breakdown with volume, RSI oversold with no reversal

## Output Template

```
### Layer 5: 기술적 분석 [🟢/🟡/🔴]

**추세 상태:**
- 현재가: $XX
- 20일 이평선: $XX [위/아래]
- 50일 이평선: $XX [위/아래]
- 200일 이평선: $XX [위/아래]
- 골든크로스/데드크로스: [해당 여부]

**모멘텀:**
- RSI (14): XX → [과매수/건강/약세/과매도]
- MACD: [골든/데드 크로스, 다이버전스 여부]
- 다이버전스: [있음/없음 — 설명]

**거래량**: [확인/비확인] — [설명]

**주요 지지/저항:**
- 지지선: $XX, $XX
- 저항선: $XX, $XX

**차트 패턴**: [패턴명] → [의미]

**매매 타이밍 제안:**
- 매수 구간: $XX ~ $XX
- 손절 기준: $XX (XX% 하락)
- 목표가 1차: $XX (XX% 상승)
- 목표가 2차: $XX (XX% 상승)
```
