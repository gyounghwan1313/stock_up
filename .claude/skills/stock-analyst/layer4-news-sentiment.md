# Layer 4: News & Sentiment Analysis

> "Be fearful when others are greedy, and greedy when others are fearful."
> — Warren Buffett

## Purpose

Understand the current market narrative around a stock. Narratives
drive short-term price action and can create opportunities when
sentiment diverges from fundamentals.

## News Categories & Impact Assessment

### Category 1: Earnings & Guidance

The single most impactful recurring event for any stock.

**What to Analyze:**
- EPS beat/miss vs consensus: By how much?
- Revenue beat/miss: Top-line growth matters for narrative
- Guidance: Raised, maintained, or lowered?
- Margin trends: Expanding or contracting?
- Management tone on earnings call: Confident or cautious?

**Impact Framework:**
| Scenario | Typical Market Reaction |
|----------|----------------------|
| Beat + Raise guidance | Strong positive (gap up) |
| Beat + Maintain guidance | Mild positive |
| Beat + Lower guidance | Negative (sell the news) |
| Miss + Maintain guidance | Mild negative |
| Miss + Lower guidance | Strong negative (gap down) |

**Buffett's Insight**: A single quarter miss in a great company
is often a buying opportunity. Look at the 3-5 year earnings trajectory,
not the latest quarter in isolation.

### Category 2: Industry & Regulatory News

- New regulations (positive or negative for the company?)
- Antitrust actions or investigations
- Government contracts or subsidies
- Industry disruption or technological shifts
- Competitor moves (M&A, product launches, failures)

### Category 3: Company-Specific Events

- M&A announcements (acquirer usually dips, target pops)
- Management changes (CEO departure = uncertainty)
- Product launches or failures
- Insider buying/selling patterns
- Share buyback announcements
- Dividend changes
- Lawsuits or legal settlements
- Accounting restatements (red flag)

### Category 4: Macro News Impact on the Stock

- How does the stock react to Fed decisions?
- Sensitivity to trade policy / tariffs?
- Currency exposure for multinationals
- Commodity price sensitivity

## Sentiment Analysis Framework

### Quantitative Sentiment Indicators

| Indicator | Bullish | Neutral | Bearish |
|-----------|---------|---------|---------|
| Analyst consensus | > 70% Buy | Mixed | > 50% Hold/Sell |
| Short interest | < 3% of float | 3-10% | > 10% (potential squeeze too) |
| Put/Call ratio | < 0.7 | 0.7-1.0 | > 1.0 |
| Analyst price target vs current | > 20% upside | ±10% | > 10% downside |
| Insider activity (3 months) | Net buying | No activity | Net selling |
| Institutional ownership change | Increasing | Stable | Decreasing |

### Qualitative Sentiment Assessment

**Media Narrative**: What's the dominant story about this company?
- "Growth juggernaut" (potential overvaluation)
- "Turnaround story" (high risk, high reward)
- "Boring dividend payer" (potential hidden value)
- "Controversial / under attack" (fear = opportunity?)

**Social Media / Retail Sentiment**: Reddit (WSB, stock subs),
Twitter/X fintwit — gauge retail enthusiasm or fear.
Extreme retail enthusiasm is often a contrarian sell signal.

**Contrarian Opportunity Checklist:**
A potential contrarian buy exists when:
1. Fundamentals are solid (Layer 2 = 🟢)
2. Sentiment is extremely negative
3. The bad news is temporary, not structural
4. Insiders are buying
5. The stock has dropped significantly from highs

## Event-Driven Analysis

For specific catalysts, assess timing and impact:

```
Event: [Description]
Expected Date: [When]
Probability: [High / Medium / Low]
Impact if Positive: [Price impact estimate]
Impact if Negative: [Price impact estimate]
Pre-positioned: [Is this already priced in? Y/N]
```

## Scoring Rubric

🟢 **Positive Sentiment Tailwind** (3 points): Favorable news flow,
   improving analyst sentiment, insider buying, positive catalyst ahead

🟡 **Mixed/Neutral Sentiment** (2 points): No strong narrative either
   way, or conflicting signals

🔴 **Negative Sentiment Headwind** (1 point): Negative news cycle,
   analyst downgrades, insider selling, uncertainty ahead

**Important Note**: Red sentiment can be a buying opportunity if
Layers 1-3 are green. This is where Buffett's "be greedy when
others are fearful" comes in. Flag this explicitly in the analysis.

## Output Template

```
### Layer 4: 뉴스 & 센티먼트 [🟢/🟡/🔴]

**최근 주요 뉴스:**
1. [날짜] [제목] → [긍정/중립/부정] — [영향 분석]
2. [날짜] [제목] → [긍정/중립/부정] — [영향 분석]
3. [날짜] [제목] → [긍정/중립/부정] — [영향 분석]

**센티먼트 지표:**
- 애널리스트 컨센서스: [Buy XX% / Hold XX% / Sell XX%]
- 목표가 평균: $XX (현재가 대비 XX%)
- 공매도 비율: XX%
- 내부자 거래: [순매수 / 중립 / 순매도]

**현재 시장 내러티브**: [어떤 스토리가 지배적인가]
**내러티브 vs 펀더멘털 괴리**: [일치 / 괴리 있음 — 설명]

**다가오는 카탈리스트:**
- [이벤트] (예상일: XX) → 예상 영향: [설명]

**역발상 기회?**: [Y/N — 근거]
```
