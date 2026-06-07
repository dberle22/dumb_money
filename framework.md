# Investment Analysis Tool: Framework Context

> This document summarizes the design framework developed prior to build. Use it as a reference when comparing to existing repo modules, identifying gaps, and planning future work.

---

## 1. Goals and Investor Profile

### Investor Profile
- **Style**: GARP (Growth at a Reasonable Price) -- a hybrid of value and growth investing with medium-term sector momentum as a supplementary lens
- **Time horizon**: Two-track approach
  - **Conviction/compounder holds**: 5-10+ years, set-and-forget unless a major fundamental shift occurs
  - **Opportunistic plays**: 1-3 years, actively managed with defined entry/exit criteria
- **Primary question**: Is this a great business at a fair price (conviction) or is this mispriced right now and why would that correct (opportunistic)?

### Tool Goals
- Evaluate an individual stock across multiple frameworks and produce a structured decision brief
- Input a sector and get an overview of key players, over/undervalued names, and broader market fit
- Screen and discover candidates before full evaluation
- Analyze and project based on macro and sector trends
- Overlay any evaluation against existing portfolio context before deciding

---

## 2. Product Modes

### Mode 1: Stock Evaluation
Input a ticker. Produce a full three-lens evaluation plus portfolio overlay and decision brief.

### Mode 2: Sector Evaluation
Input a sector. Produce a landscape of key players, relative valuations, momentum positioning, and fit into the broader market.

### Mode 3: Market Overview
Macro-level view across sectors. Identify rotation signals, sector momentum, and broad valuation context.

### Mode 4: Discovery and Screening (upstream of evaluation)
Two sub-modes:
- **Peer Comparison**: Input a stock, surface closest comparables by sub-industry and size, run side-by-side metrics across all three lenses
- **Sector Screener**: Input a sector or thesis, surface top candidates ranked independently by value, growth, and momentum screens
- **Historical Analogs** (stretch goal): Find periods where a sector or stock looked similar to today on key metrics and show what happened next -- requires normalized historical time-series

---

## 3. Evaluation Module: Multi-Lens Architecture

Each evaluation runs three independent lenses, synthesizes them, then applies a portfolio overlay. The tool produces a decision brief -- it does not make the buy/sell decision.

### Lens 1: Value
**Question**: Is the stock cheap relative to intrinsic value and sector peers?

Key metrics:
- EV/EBITDA vs. sector median
- Price to free cash flow (P/FCF)
- Price to book (P/B)
- Dividend yield (where relevant)
- DCF-derived intrinsic value with explicit discount rate and terminal growth rate assumptions

Output: Undervalued / fairly valued / overvalued, with margin of safety estimate

Bear case -- value trap check:
- Is revenue declining or margins compressing over the last 3 years?
- Is debt rising while FCF is flat or falling?
- Is the stock cheap because the whole sector is in secular decline?

### Lens 2: Growth
**Question**: Is the business growing and can it sustain that growth?

Key metrics:
- Revenue CAGR over 1, 3, and 5 years
- Gross margin and operating margin trends
- PEG ratio (P/E divided by earnings growth rate)
- Earnings revision direction (consensus moving up or down)
- R&D and capex as a signal of investment in future growth
- TAM context (qualitative, sourced from earnings calls or research)

Output: High / medium / low growth quality with a durability flag

Bear case -- assumption break check:
- What growth rate is implied by the current price? Back-solve the DCF.
- Is gross margin expanding or compressing at scale?
- Are earnings revisions trending up or down over the last two quarters?

### Lens 3: Momentum
**Question**: Is the sector or stock in a favorable trend over a 6-12 month window?

Key metrics:
- Relative strength vs. sector and industry benchmark ETFs over 1, 3, 6, and 12 month windows
- Price trend vs. 50-day and 200-day moving averages
- Volume-weighted trend confirmation
- Short interest (FINRA, published twice monthly)
- Earnings surprise history

Output: Strong / neutral / weak momentum with a trend stability note

Bear case -- reversal check:
- Is the stock outperforming its sector while the sector weakens vs. the broad market?
- Is momentum driven by multiple expansion rather than earnings growth?
- Is short interest rising sharply while price rises?

### Synthesis Layer
Compare the three verdicts -- do not average them. Surface agreement and conflict explicitly.

| Pattern | Interpretation |
|---|---|
| All three positive | Strong conviction signal -- rare but high confidence |
| Value + growth positive, momentum weak | Early-cycle opportunity, may require patience |
| Momentum strong, value + growth mixed | Potentially late to the trade -- be cautious |
| Conflict across all three | Flag as research gap, do not force a signal |

The synthesis layer produces a tension map, not a composite score. Composite scores collapse nuance needed for a good decision.

### Portfolio Overlay Layer
Applied after the three-lens synthesis. Asks:
- **Correlation**: Does this correlate highly with existing holdings? Concentration vs. diversification.
- **Sector exposure**: How does adding this shift sector weight distribution?
- **Horizon fit**: Does this match the opportunistic or conviction portion of the portfolio?

Output: Sizing and timing guidance, not a valuation judgment.

Portfolio bear case:
- If this thesis is wrong, which other holdings would also be hurt?
- Does this push any sector above an acceptable concentration threshold?

### Decision Brief (final output)
- Lens verdicts and where they agree or conflict
- The one or two assumptions the thesis depends on most
- Portfolio fit assessment
- Suggested position sizing range
- What would change the thesis -- exit and monitoring criteria

---

## 4. Data Requirements

### Currently Available
- Daily stock prices
- Sector and industry ETF benchmark prices (multi-year history)
- Company earnings, balance sheets, and financial metadata
- Portfolio holdings with position sizes and cost basis (separate module)

### Required -- Not Yet Ingested
| Data | Use | Suggested Source |
|---|---|---|
| Analyst consensus estimates (forward EPS, revenue) | Growth lens, earnings revision signal | Alpha Vantage, Financial Modeling Prep, Polygon.io |
| Short interest | Momentum lens, bear case | FINRA (free, twice-monthly files) |
| Earnings call transcripts | Qualitative growth and bear case signals | Seeking Alpha, Motley Fool, Earnings Whispers APIs |
| Industry and company research | Moat assessment, TAM, competitive positioning | Manual or LLM-assisted summarization |
| News and social media | Risk flagging only -- not a primary signal | News APIs (lower priority) |
| Volume history | Momentum lens -- trend confirmation | May already exist alongside price data |

### Derived Data to Build
- EV (market cap + debt - cash) time series
- Free cash flow (operating cash flow minus capex)
- ROIC (return on invested capital) -- strongest single indicator of business quality
- Relative strength index vs. benchmark ETFs over multiple windows
- Correlation matrix across holdings
- Normalized historical time-series for analog matching

---

## 5. Key Design Principles

**Valuation sets the eligible universe. Portfolio context governs position sizing and entry timing.** These answer different questions and should never be conflated.

**No single composite scores.** Each lens produces an independent verdict. Forcing them into one number destroys the analytical value of running multiple frameworks.

**Bear case is not a separate module.** It lives inside each lens evaluation and surfaces in the decision brief. The question on each lens is always: what would have to be true for this to be wrong?

**Two evaluation modes drive different outputs.** Conviction/compounder mode emphasizes ROIC, business quality, and durable advantage. Opportunistic mode emphasizes relative valuation, earnings revisions, and near-term catalysts.

**Historical data is an asset -- use it.** Multi-year price and benchmark history enables relative strength calculations, historical analog matching, and trend durability assessment. Most retail tools don't have this.

---

## 6. Known Gaps and Open Questions

- Analyst consensus estimates are the most critical missing data point for the growth lens. Identify whether the existing earnings data source provides forward estimates.
- Earnings call transcripts are the most important qualitative gap. Not required to launch but needed for the full framework.
- The synthesis layer logic -- how to surface lens conflict clearly without paralyzing the user -- is the hardest UX and logic design challenge. Prototype with real examples before finalizing the format.
- Momentum lens quality depends on how granular the benchmark ETFs are. Sector-level is the minimum; sub-industry level is better.
- TAM and competitive positioning cannot be derived from financial data alone. Plan for a qualitative input field or LLM-assisted summarization from earnings call text.
- DCF discount rate and terminal growth rate are user-defined assumptions, not data inputs. The tool should make these explicit and visible, not bury them.

---

## 7. Suggested Build Sequence

1. Lock in the two evaluation modes (conviction vs. opportunistic) and what each lens emphasizes per mode
2. Build the fundamental quality and valuation layer using existing price, balance sheet, and earnings data
3. Add derived metrics: FCF, ROIC, EV, relative strength windows
4. Build sector comparison before absolute valuation -- relative analysis is more actionable early
5. Ingest short interest data (low effort, meaningful signal)
6. Add analyst consensus estimates for the growth lens and revision signal
7. Build the synthesis and decision brief output layer
8. Add peer comparison and sector screener (discovery mode)
9. Add earnings call transcript ingestion for qualitative layer
10. Add historical analog matching as a stretch feature