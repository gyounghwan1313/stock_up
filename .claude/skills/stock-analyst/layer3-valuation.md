# Layer 3: Valuation Analysis

> "Price is what you pay. Value is what you get." — Warren Buffett

## Purpose

Determine whether the current stock price offers a margin of safety.
Even the best business is a bad investment if you overpay.

## Valuation Methods

Use multiple methods and triangulate. No single method is reliable alone.

### Method 1: Relative Valuation (Multiples)

Compare the stock's current multiples to (a) its own history,
(b) industry peers, and (c) the broader market.

**Core Multiples:**

| Multiple | Formula | Best For | Caution |
|----------|---------|----------|---------|
| P/E (TTM) | Price / EPS | Profitable companies | Earnings can be manipulated |
| Forward P/E | Price / Est. EPS | Growth companies | Depends on analyst accuracy |
| PEG | P/E / EPS Growth% | Growth at reasonable price | Growth rate is a guess |
| P/B | Price / Book Value | Banks, asset-heavy | Irrelevant for asset-light |
| EV/EBITDA | Enterprise Value / EBITDA | Capital structure neutral | Ignores CapEx differences |
| P/S | Price / Revenue | Pre-profit companies | Ignores profitability |
| P/FCF | Price / Free Cash Flow | Cash-generating companies | Most reliable for mature cos |
| EV/Sales | Enterprise Value / Revenue | Cross-sector comparison | Very rough metric |

**How to Judge:**

```
                   Undervalued        Fair Value       Overvalued
Current vs 5Y avg:    < -1σ           ±1σ              > +1σ
Current vs peers:     < 75th pct      25-75th pct      > 25th pct
Current vs S&P 500:   Discount        In-line          Premium
```

σ = standard deviation from the 5-year mean.

**PEG Ratio Interpretation:**
- PEG < 1.0: Potentially undervalued relative to growth
- PEG 1.0-2.0: Fairly valued
- PEG > 2.0: Growth already priced in, potentially expensive

### Method 2: DCF (Discounted Cash Flow)

The theoretically correct way to value any asset. Build a simple
3-scenario DCF when data is available.

**Simplified DCF Steps:**
1. Start with current FCF
2. Project FCF growth for 5-10 years (use 3 scenarios)
3. Apply terminal growth rate (2-3% for most companies)
4. Discount back at WACC (or use 10% as a simple hurdle rate)
5. Divide total present value by shares outstanding

**Scenario Table:**
| Scenario | FCF Growth (Y1-5) | FCF Growth (Y6-10) | Terminal Growth | Weight |
|----------|-------------------|---------------------|-----------------|--------|
| Bull     | Higher estimate    | Moderate             | 3%              | 25%    |
| Base     | Consensus estimate | Lower                | 2.5%            | 50%    |
| Bear     | Lower estimate     | Flat/declining       | 2%              | 25%    |

**Weighted Fair Value** = (Bull × 0.25) + (Base × 0.50) + (Bear × 0.25)

**Discount rate guidance:**
- Large cap, stable: 8-10%
- Mid cap, moderate risk: 10-12%
- Small cap, high risk: 12-15%
- Buffett uses a 10% hurdle as his minimum acceptable return

### Method 3: Earnings Power Value (EPV)

Buffett's preferred mental model. What is the company worth based
on current earnings power, assuming no growth?

```
EPV = Adjusted Earnings / Cost of Capital
```

- If EPV > Market Cap: You're getting growth for free
- If EPV < Market Cap: You're paying for future growth (risky)

### Method 4: Reverse DCF

Instead of projecting forward, ask: "What growth rate is the market
currently pricing in?"

If the implied growth rate seems unrealistic (e.g., 25% for 10 years
for a large cap), the stock is likely overvalued.

## Margin of Safety

> "The three most important words in investing: margin of safety."

**Calculation:**
```
Margin of Safety = (Intrinsic Value - Current Price) / Intrinsic Value × 100
```

**Buffett's Standard:**
| Risk Level | Required Margin of Safety |
|------------|--------------------------|
| Blue chip, wide moat | 15-25% |
| Good company, some risk | 25-40% |
| Turnaround / uncertain | 40-50%+ |

## Common Valuation Traps

- **Value trap**: Cheap multiples but deteriorating business (check Layer 2)
- **Growth trap**: High growth priced at extreme multiples (check PEG, reverse DCF)
- **Cyclical trap**: Low P/E at cycle peak = actually expensive
  (for cyclicals, buy at high P/E near trough, sell at low P/E near peak)
- **Debt trap**: Low P/E but high EV/EBITDA due to leverage

## Scoring Rubric

🟢 **Attractive Valuation** (3 points): Trading below intrinsic value
   with > 20% margin of safety, multiples below historical average

🟡 **Fair Valuation** (2 points): Near intrinsic value, multiples
   in-line with history, limited margin of safety

🔴 **Overvalued** (1 point): Priced above intrinsic value, elevated
   multiples, market pricing in aggressive growth

## Output Template

```
### Layer 3: 밸류에이션 [🟢/🟡/🔴]

**주요 멀티플:**
| 지표 | 현재 | 5Y 평균 | 업종 평균 | 판정 |
|------|------|---------|----------|------|
| P/E (TTM) | XX | XX | XX | [저/적정/고] |
| Forward P/E | XX | XX | XX | [저/적정/고] |
| PEG | XX | - | XX | [저/적정/고] |
| EV/EBITDA | XX | XX | XX | [저/적정/고] |
| P/FCF | XX | XX | XX | [저/적정/고] |

**DCF 적정가치 (3-시나리오):**
- Bull Case: $XX (확률 25%)
- Base Case: $XX (확률 50%)
- Bear Case: $XX (확률 25%)
- **가중평균 적정가치: $XX**

**현재가: $XX**
**안전마진: XX%**
→ 판정: [매력적 / 적정 / 고평가]

**역 DCF 내재 성장률**: XX% (현실적인가? [Y/N])
```
