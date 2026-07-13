# Data Sources and KPI Catalog

This document maps data types to their sources, availability status, and the KPIs they enable. The organizing principle is **data type first, source second** — if you want to know where earnings revision data comes from, look under "Analyst Estimates," not under "yfinance."

Companion references:
- [data_models.md](data_models.md) — canonical table schemas and storage contracts
- [architecture.md](architecture.md) — pipeline structure and module map
- [stock_scorecard_spec.md](stock_scorecard_spec.md) — scorecard metric definitions and weights
- [framework.md](../framework.md) — three-lens evaluation framework and investor profile

---

## Status Key

| Status | Meaning |
|---|---|
| `built` | Ingested, staged into DuckDB, and consumed by analytics or reports |
| `available` | Data exists in the provider; ingestion module not yet written |
| `planned` | Scheduled for a specific sprint |
| `future` | Valuable but not yet on the roadmap |
| `manual` | No programmatic source; requires user input |

---

## 1. Price and Volume Data

**What it is:** Daily OHLCV bars for individual securities and benchmark ETFs.

**Status:** `built`

**Current source:** yfinance (`yf.Ticker.history()`)

**What we ingest:**
- Open, high, low, close, adjusted close, volume
- Daily bars going back as far as the provider supports (typically 20+ years for large caps)
- Both individual company tickers and benchmark ETFs (SPY, QQQ, IWM, XLK, etc.)

**Stored in:** `normalized_prices` (DuckDB)

**KPIs enabled:**
- Trailing returns over 1m, 3m, 6m, 1y, 3y windows
- Drawdown series and max drawdown
- 50-day and 200-day simple moving averages
- Price vs. 200-day MA signal
- Relative strength vs. benchmark ETFs
- Volume-weighted trend confirmation
- Indexed price series for benchmark comparison charts
- Beta (vs. SPY or sector ETF)
- Volatility (annualized standard deviation of returns)
- Sharpe ratio

**Alternative sources:**
- Polygon.io — free tier available, higher rate limits than yfinance, real-time data on paid tier
- Alpha Vantage — free tier available, slower, coverage is comparable
- EODHD — paid, strong global coverage

**Notes:**
- Volume coverage across the full IWM universe has been validated; gaps are minor and isolated
- Adjusted close is the canonical price field; unadjusted close is preserved as a fallback

---

## 2. Financial Statements (Historical Fundamentals)

**What it is:** Quarterly and annual income statement, balance sheet, and cash flow statement data going back multiple fiscal years.

**Status:** `built`

**Current source:** yfinance (`yf.Ticker.financials`, `quarterly_financials`, `balance_sheet`, `quarterly_balance_sheet`, `cashflow`, `quarterly_cashflow`)

**What we ingest:**
- Revenue, gross profit, operating income, net income, EBITDA
- Free cash flow, operating cash flow, capex
- Total debt, total cash, current assets, current liabilities
- Stockholders equity, invested capital, working capital
- Basic EPS, diluted EPS
- Interest expense, tax provision, effective tax rate

**Stored in:** `normalized_fundamentals` (DuckDB) — one row per ticker × period, with `period_type` values of `quarterly`, `annual`, and `ttm`

**KPIs enabled:**
- Revenue CAGR over 1, 3, and 5 years
- Gross margin, operating margin, profit margin, FCF margin — point-in-time and trend
- ROIC (NOPAT / invested capital) and ROE
- Net debt (total debt − total cash)
- Net debt / EBITDA
- Current ratio, debt-to-equity
- FCF to debt ratio
- EPS growth rate
- Interest coverage (EBIT / interest expense)
- Margin expansion or compression over time

**Alternative sources:**
- Macrotrends — free but scraping only, no API
- Simplywall.st — no public API
- Financial Modeling Prep (FMP) — paid tiers, cleaner normalized statements, longer history, more consistent field naming across companies
- Polygon.io — financials on paid tier

**Notes:**
- yfinance field naming is inconsistent across companies and sectors; the ingestion layer applies field aliases to handle this
- ROIC derivation requires `invested_capital` or `total_equity + total_debt`, both of which are available but coverage varies by company
- Financial companies (banks, insurers) use different statement structures — balance sheet metrics like debt-to-equity are not comparable and should be excluded from generic scoring

---

## 3. Company Metadata and Security Master

**What it is:** Descriptive fields for each security — name, sector, industry, exchange, country, asset type.

**Status:** `built`

**Current sources:**
- yfinance `info` dict (sector, industry, country, long name, website)
- Nasdaq Trader listed-security files (exchange listing, active status, ETF flag)
- Manual overrides (`security_master_overrides`)

**Stored in:** `security_master` (DuckDB)

**KPIs enabled:**
- Peer group construction (same industry or same sector)
- Benchmark assignment by sector and industry
- Research universe eligibility filtering
- Display metadata in reports

**Alternative sources:**
- OpenFIGI — free API for instrument identification and sector/asset-type lookup
- SEC EDGAR company search — free, covers all SEC-registered companies with CIK and SIC codes

---

## 4. Analyst Estimates (Forward EPS and Revenue)

**What it is:** Sell-side analyst consensus estimates for forward earnings per share and revenue, typically covering the current quarter, next quarter, current fiscal year, and next fiscal year.

**Status:** `available` (Sprint 10 planned)

**Current source decision:** yfinance — all endpoints below are free and require no API key

**What yfinance provides:**

| Endpoint | Fields | Coverage |
|---|---|---|
| `t.earnings_estimate` | Forward EPS avg/low/high, analyst count, YoY growth | Current Q, next Q, current year, next year |
| `t.revenue_estimate` | Forward revenue avg/low/high, analyst count, YoY growth | Current Q, next Q, current year, next year |
| `t.eps_trend` | EPS consensus drift over 7, 30, 60, and 90 days | Current Q, next Q, current year, next year |
| `t.eps_revisions` | Up/down revision counts over last 7 and 30 days | Current Q, next Q, current year, next year |
| `t.earnings_history` | Actual vs. estimate, surprise % | Last 4 reported quarters |
| `t.analyst_price_targets` | Current, high, low, mean, median price target | Point-in-time snapshot |
| `t.recommendations_summary` | Strong buy, buy, hold, sell, strong sell counts | Last 4 months |
| `t.growth_estimates` | Long-term growth estimate vs. index | Point-in-time |

**Planned table:** `forward_estimates` (DuckDB) — one row per ticker × snapshot date × period, separate from `normalized_fundamentals`

**KPIs enabled:**
- Forward EPS and forward revenue consensus
- PEG ratio (forward P/E ÷ long-term growth estimate)
- Earnings revision direction signal (revisions trending up vs. down — growth lens input)
- Estimate momentum (how far consensus has moved in 30/60/90 days)
- Earnings surprise history (beat/miss rate and magnitude — momentum lens input)
- Analyst price target vs. current price (implied upside)
- Buy/sell/hold distribution

**Alternative sources:**
- Financial Modeling Prep (FMP) — stronger: longer estimate history (5+ years forward), more granular revision data, analyst coverage count by sector. Free tier: 250 calls/day, 5/min. Sufficient for a personal watchlist; better than yfinance if longer-horizon estimates become important for DCF
- Alpha Vantage — free tier available but estimates data is limited
- Polygon.io — financials and estimates on paid tier
- Refinitiv (LSEG) / Bloomberg — institutional, expensive
- Visible Alpha — institutional, expensive but very detailed segment-level estimates

**Decision (2026-06-27):** Use yfinance for all estimate data in active Sprint 10 and 11 work. FMP is not needed for the initial personal-watchlist workflow. yfinance covers everything currently in scope (forward EPS/revenue by quarter, revision counts, eps trend, earnings surprise history, price targets). Keep FMP as a documented backup option only if yfinance estimate coverage proves unreliable or if 5-year forward projections become necessary for later DCF sensitivity work.

---

## 5. Short Interest

**What it is:** The number of shares sold short and derived metrics (short % of float, days to cover), published twice monthly by FINRA.

**Status:** `planned` (Sprint 10)

**Current source:** FINRA — free, no API key required, published as downloadable flat files twice per month (settlement dates approximately the 15th and end of month)

**What FINRA provides:**
- Short interest shares (total shares sold short at settlement date)
- Settlement date
- Published by exchange (NYSE, NASDAQ, OTC)

**Derived fields to stage:**
- `short_interest_change` — shares change from prior period
- `short_interest_change_pct` — percent change in short interest
- `short_interest_pct_float` — short interest as a percent of float (requires shares float from security master or fundamentals)
- `days_to_cover` — short interest ÷ average daily volume (requires price volume)

**Planned table:** `short_interest` (DuckDB) — one row per ticker × report date

**KPIs enabled:**
- Short interest level (absolute and vs. float)
- Short interest trend (rising vs. falling)
- Days to cover (squeeze potential)
- Momentum bear case: is short interest rising while price rises?

**Alternative sources:**
- Finviz — displays short data but no API
- MarketBeat — aggregated short interest, no free API
- YCharts — paid
- yfinance `t.info` — provides `shortRatio` (days to cover) and `shortPercentOfFloat` as point-in-time snapshots from Yahoo Finance, but no history and no twice-monthly cadence. Useful as a quick sanity check but not a substitute for FINRA history

**Notes:**
- FINRA files are the authoritative free source for short interest history; no API key or registration required
- FINRA OTC file and exchange files are separate downloads; both needed for full coverage
- Automated download is straightforward but should be added after the local-file ingestion contract is validated on the watchlist

---

## Planning Note

For the near-term roadmap, the intended sequence is:

1. Sprint 7 portfolio foundation
2. Sprint 10 data expansion
3. Sprint 11 lens framework
4. Sprint 12 single-ticker decision brief

The broader SQL-first gold-layer rebuild remains an adjacent evolution track. It should improve the reusable analytical surface over time, but it is not a prerequisite to begin Sprints 7 or 10-12.

---

## 6. Valuation Snapshot Data

**What it is:** Point-in-time valuation multiples derived from the yfinance `info` dict — these are calculated by the provider rather than derived by us from statements.

**Status:** `built`

**Current source:** yfinance `info` dict

**What we ingest:**
- Forward P/E, trailing P/E
- Price-to-sales (TTM)
- EV/EBITDA
- Enterprise value, market cap
- Dividend yield
- Forward EPS, trailing EPS (single value, not the full estimate table)

**Stored in:** `normalized_fundamentals` as the latest-snapshot TTM row

**KPIs enabled:**
- Absolute valuation scoring (forward P/E, EV/EBITDA, P/S)
- FCF yield (derived: FCF ÷ market cap)
- Peer-relative valuation comparison (vs. peer median)

**Notes:**
- The provider-calculated multiples in the `info` dict are convenient but less auditable than deriving multiples from staged financial statement data; for the three-lens value work, we may want to derive P/FCF and EV/EBITDA ourselves from staged fundamentals to ensure consistency

---

## 7. DCF and Scenario Configuration

**What it is:** User-defined assumptions for discounted cash flow valuation — not a data source but a config layer that governs how intrinsic value is calculated.

**Status:** `planned` (Sprint 10)

**Source:** `AppSettings` (config file, not a provider)

**Planned fields:**
- `dcf_discount_rate` — default 8% (WACC proxy)
- `dcf_terminal_growth_rate` — default 3%
- Bear/base/bull scenario overrides without requiring code changes

**KPIs enabled:**
- DCF intrinsic value (bear, base, bull)
- Margin of safety (intrinsic value vs. current price)
- Implied growth rate back-solve (what growth rate does the current price assume?)

**Notes:**
- DCF fallback order: use forward EPS/revenue estimates when available, otherwise fall back to historical FCF CAGR
- Discount rate and terminal growth rate are visible, explicit assumptions — never buried

---

## 8. Benchmark and ETF Holdings

**What it is:** Constituent membership lists for benchmark ETFs (SPY, QQQ, IWM, XLK, etc.) — who is in the basket and with what weight.

**Status:** `built`

**Current source:** Manually downloaded ETF holdings files (from ETF provider websites — iShares, Vanguard, SPDR)

**Stored in:** `benchmark_memberships`, `benchmark_definitions`, `benchmark_mappings` (DuckDB)

**KPIs enabled:**
- Benchmark assignment per ticker (primary, sector, industry, style)
- Peer group seeding from benchmark membership overlap
- Universe ingestion prioritization (ingest IWM members first)

**Alternative sources:**
- ETF.com — constituent data available for download
- yfinance `t.info` for ETF tickers — provides top holdings but not full constituent list

**Notes:**
- Holdings files need to be refreshed periodically as ETF constituents change; no automated refresh exists yet

---

## 9. SEC Filings

**What it is:** Official company filings submitted to the SEC — 10-K (annual), 10-Q (quarterly), 8-K (material events), proxy statements, and others.

**Status:** `future`

**Source:** SEC EDGAR — completely free, no API key required

**Key endpoints:**
- `https://data.sec.gov/submissions/{CIK}.json` — filing history for a company
- `https://data.sec.gov/api/xbrl/companyfacts/{CIK}.json` — structured XBRL financial data going back decades
- Full-text search: `https://efts.sec.gov/LATEST/search-index?q=...`

**What's available:**
- Structured financial data via XBRL (same income statement, balance sheet, cash flow fields as yfinance but often with longer history and more consistent field names)
- 10-K and 10-Q full text (unstructured)
- 8-K filings (earnings press releases, material events)
- Insider transactions (Form 4)
- Institutional ownership (Form 13F)
- Proxy statements (DEF 14A) — executive compensation, board composition

**KPIs enabled:**
- Longer financial statement history than yfinance provides (EDGAR XBRL goes back to ~2009 for most filers)
- Insider buying and selling signals
- Institutional ownership concentration and changes
- Executive compensation trends (qualitative, labor-intensive)

**Notes:**
- EDGAR XBRL is the most underused free financial data source available; field coverage and history are excellent for SEC-registered companies
- `CIK` (Central Index Key) can be looked up from ticker via `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=AAPL&type=10-K`
- `security_master` already has a `cik` field reserved for this join

---

## 10. Institutional Ownership (13F Filings)

**What it is:** Quarterly snapshots of institutional holdings — hedge funds, mutual funds, pension funds, and other large investors must disclose positions over $100M.

**Status:** `future`

**Sources:**
- SEC EDGAR — free; Form 13F filings, published 45 days after quarter end
- WhaleWisdom — aggregated 13F data with change tracking, free tier available
- Fintel — 13F aggregation with institution-level drill-down, limited free tier

**KPIs enabled:**
- Institutional ownership concentration (% held by top 10 institutions)
- Institutional ownership change quarter-over-quarter (accumulation vs. distribution signal)
- Number of institutional holders
- Hedge fund sentiment (which funds are buying or selling)

**Notes:**
- 45-day lag means 13F data is always stale relative to current positioning
- Most useful as a background context signal rather than a trading signal
- EDGAR free access is sufficient; aggregators add convenience but not unique data

---

## 11. News and Sentiment

**What it is:** Recent news articles, headlines, and press releases about a company.

**Status:** `available` (low priority)

**Current source:** yfinance `t.news` — returns approximately 10 recent articles with title, summary, publication date, and source

**What yfinance provides:**
- Title, summary, publication date, provider name
- Link to full article (no body text)
- ~10 articles, no configurable lookback

**Alternative sources:**
- NewsAPI — free tier (100 requests/day), returns full article metadata and body text for many sources
- Alpha Vantage News Sentiment — free tier, includes sentiment scores per article
- Polygon.io News — free tier available, broader coverage
- Financial Times, Bloomberg, WSJ — no free API; content paywalled
- RSS feeds (Yahoo Finance, Reuters, MarketWatch) — free, no API key, headline + summary only, easy to parse

**KPIs enabled:**
- News volume (event detection signal)
- Sentiment scoring (positive/negative/neutral — useful as a bear case flag, not a primary signal)
- Earnings announcement monitoring
- Material event detection (8-K proxy, less reliable)

**Notes:**
- News is classified as a risk-flagging signal only in the framework — not a primary lens input
- Avoid building sentiment scoring before the structured data foundation is complete; narrative signals are not worth the noise at this stage
- yfinance's 10-article snapshot is sufficient for a "recent news" display in the decision brief; full sentiment analysis requires a dedicated news source

---

## 12. Earnings Call Transcripts

**What it is:** Full text of quarterly earnings conference calls — management commentary, analyst Q&A, and forward guidance language.

**Status:** `future`

**Potential sources:**
- Seeking Alpha — transcripts published within hours of calls, readable via web scraping (fragile, ToS risk)
- Motley Fool — transcripts available with some delay, web scraping only
- Earnings Whispers — focused on earnings dates and surprise history, limited transcript access
- The Motley Fool Transcripts API — limited documentation, unclear availability
- Isabelnet — aggregates some transcripts
- Roic.ai — includes some transcript summaries
- Financial Modeling Prep (FMP) — earnings call transcripts on paid tier (~$15/mo for starter tier)
- Refinitiv / Bloomberg — institutional, expensive

**KPIs enabled (qualitative, requires LLM processing):**
- Management tone and confidence signals
- Forward guidance language (raised, maintained, or lowered)
- TAM framing and competitive positioning language
- Bear case language identification (what risks did management acknowledge?)
- R&D investment intent signals

**Notes:**
- Transcripts are the most important qualitative gap in the framework but are explicitly deferred until the structured data foundation is mature (per framework.md §6)
- The most practical near-term approach is FMP paid tier (~$15/mo) or a scraping approach against Seeking Alpha — neither is clean
- LLM summarization is the right processing layer when transcripts are ingested (Sprint 9 or later)
- Earnings call dates are available today via `t.calendar` from yfinance

---

## 13. Insider Transactions

**What it is:** Form 4 filings showing purchases and sales by company officers, directors, and 10%+ shareholders.

**Status:** `future`

**Sources:**
- SEC EDGAR Form 4 — free, no API key, filed within 2 business days of transaction
- OpenInsider — aggregated Form 4 data, free web interface, no public API
- Finviz — displays insider transaction history, no free API
- yfinance `t.insider_transactions` — available, returns recent Form 4 data as a DataFrame

**KPIs enabled:**
- Insider buying as a conviction signal (especially cluster buying)
- Insider selling as a distribution signal (less reliable — executives sell for many reasons)
- Net insider transaction direction over trailing 6 months

**Notes:**
- yfinance already surfaces insider transaction data; this could be added with minimal effort
- Insider buying near a 52-week low is one of the higher-quality signals available from public data
- Cluster buying (multiple insiders buying in the same quarter) is a stronger signal than single transactions

---

## 14. Macroeconomic and Market Data

**What it is:** Broad market indicators, interest rates, inflation, credit spreads, and sector rotation signals.

**Status:** `future`

**Sources:**
- FRED (Federal Reserve Economic Data) — completely free, no API key required for most series; `fredapi` Python library available
  - Series: Fed Funds Rate, 10Y Treasury yield, CPI, PCE, unemployment, ISM, credit spreads
- yfinance — treasury ETFs (TLT, IEF, SHY), VIX (`^VIX`), DXY (`DX-Y.NYB`), and sector ETFs already in scope
- OECD data — free, international macro indicators

**KPIs enabled (Mode 3: Market Overview):**
- Yield curve shape (2Y vs. 10Y spread)
- Credit spread environment (HYG vs. LQD or investment-grade vs. high-yield spread)
- Sector rotation signals (relative performance of sector ETFs vs. SPY)
- VIX-based risk environment flag
- Dollar strength vs. international exposure

**Notes:**
- Most macro signals can be proxied using ETFs already ingested (TLT, HYG, LQD, GLD, DXY)
- FRED is the cleanest free source for official macro data and requires almost no setup
- Macro layer is Mode 3 scope — not needed until single-stock and sector evaluation modes are stable

---

## 15. Historical Valuation Bands

**What it is:** A time series of historical P/E, EV/EBITDA, P/S, and P/FCF ratios going back 5–10 years, enabling valuation-band charts and mean-reversion context.

**Status:** `future`

**Derivation approach:** Build from normalized price history + normalized fundamentals history — no new source needed once both are staged for a full ticker history

**KPIs enabled:**
- Current multiple vs. 5-year average (is this cheap or expensive vs. its own history?)
- Valuation band chart (10th, 25th, 50th, 75th, 90th percentile)
- Mean reversion signal (multiples compressing vs. expanding vs. historical range)

**Notes:**
- This is a derived dataset, not a new ingestion requirement
- Requires joining normalized price history with historical fundamentals snapshots, which exist today
- Accuracy depends on having enough quarterly fundamentals history; yfinance typically provides 4–8 years of quarterly data for large caps

---

## 16. TAM and Competitive Intelligence

**What it is:** Total addressable market estimates, competitive positioning context, and moat assessment — qualitative signals that cannot be derived from financial data alone.

**Status:** `future` (manual or LLM-assisted)

**Sources:**
- Earnings call transcripts (see section 12) — management TAM framing
- Industry research reports — Gartner, IDC, Forrester (all paid)
- Company investor relations pages — often include market size slides
- LLM summarization of SEC filings or transcripts — most scalable approach for a personal tool
- Manual research notes (user input field)

**KPIs enabled (qualitative):**
- TAM estimate (user-defined or LLM-extracted)
- Competitive positioning description
- Moat type classification (network effect, cost advantage, switching cost, intangible asset, efficient scale)
- Key risk factors (from 10-K risk section — extractable via EDGAR)

**Notes:**
- This is the hardest gap to fill programmatically
- Near-term approach: a structured user-input field in the decision brief where you record TAM and moat notes manually
- Longer-term: LLM summarization of the 10-K business description and risk factors section using EDGAR text (zero marginal cost once transcripts are in scope)

---

## Summary: Data Availability by Lens

### Value Lens

| Input | Source | Status |
|---|---|---|
| EV/EBITDA | yfinance `info` + staged fundamentals | `built` |
| P/FCF | Derived from staged fundamentals | `built` |
| P/B | yfinance `info` | `built` |
| Dividend yield | yfinance `info` | `built` |
| DCF intrinsic value | Staged FCF + AppSettings config | `planned` (Sprint 10) |
| Net debt (for EV) | Derived: total debt − total cash | `built` |
| Forward EPS (for DCF) | yfinance `t.earnings_estimate` | `available` (Sprint 10) |
| Peer EV/EBITDA median | `peer_sets` + staged fundamentals | `built` |
| Historical valuation bands | Derived from staged price + fundamentals history | `future` |

### Growth Lens

| Input | Source | Status |
|---|---|---|
| Revenue CAGR (1/3/5yr) | Historical `normalized_fundamentals` | `built` |
| Gross/operating margin trend | Historical `normalized_fundamentals` | `built` |
| EPS growth rate | Historical `normalized_fundamentals` | `built` |
| ROIC trend | Historical `normalized_fundamentals` (NOPAT + invested capital) | `built` |
| PEG ratio | Forward EPS + LT growth estimate (yfinance `t.growth_estimates`) | `available` (Sprint 10) |
| Earnings revision direction | yfinance `t.eps_revisions` | `available` (Sprint 10) |
| EPS estimate momentum (30/90d) | yfinance `t.eps_trend` | `available` (Sprint 10) |
| R&D / capex signal | Staged fundamentals (capex from cash flow statement) | `built` |
| TAM context | Manual input or LLM-assisted | `future` |
| Implied growth back-solve | Derived from DCF + current price | `planned` (Sprint 11) |

### Momentum Lens

| Input | Source | Status |
|---|---|---|
| Relative strength vs. SPY (1/3/6/12m) | `normalized_prices` + benchmark prices | `built` |
| Relative strength vs. sector ETF (1/3/6/12m) | `normalized_prices` + `benchmark_mappings` | `built` |
| Price vs. 50d and 200d MA | `normalized_prices` | `built` |
| Volume trend confirmation | `normalized_prices` (volume field) | `built` |
| Earnings surprise history | yfinance `t.earnings_history` | `available` (Sprint 10) |
| Short interest level and trend | FINRA flat files | `planned` (Sprint 10) |
| Days to cover | FINRA + volume from `normalized_prices` | `planned` (Sprint 10) |
| Short interest % of float | FINRA + shares float from `normalized_fundamentals` | `planned` (Sprint 10) |
| Analyst price target vs. current | yfinance `t.analyst_price_targets` | `available` (Sprint 10) |

### Scorecard (existing, separate from lenses)

| Input | Source | Status |
|---|---|---|
| All market performance metrics | `normalized_prices` + `benchmark_mappings` | `built` |
| All profitability metrics | `normalized_fundamentals` | `built` |
| All balance sheet metrics | `normalized_fundamentals` | `built` |
| Valuation multiples | `normalized_fundamentals` + `gold_ticker_metrics_mart` | `built` |
| Peer-relative valuation | `peer_sets` + `normalized_fundamentals` | `built` |

---

## Source Quick Reference

| Source | Cost | Auth | What it covers | Notes |
|---|---|---|---|---|
| yfinance | Free | None | Prices, financials, metadata, estimates, news, insider data | Primary source; estimate endpoints unused until Sprint 10 |
| FINRA flat files | Free | None | Short interest (twice monthly) | Download manually or automate; no API |
| SEC EDGAR | Free | None | All SEC filings, XBRL financials, 13F, Form 4 | Most underused free source; CIK join via `security_master` |
| FRED | Free | None (optional key) | Macro indicators, rates, inflation, credit spreads | `fredapi` Python library available |
| Nasdaq Trader files | Free | None | Listed security universe, exchange metadata | Used for `security_master` seeding |
| Financial Modeling Prep | Free tier / paid | API key | Estimates (5yr), transcripts, financials | 250 calls/day free; only needed if yfinance estimate depth is insufficient |
| Alpha Vantage | Free tier / paid | API key | Prices, fundamentals, news sentiment | Slower than yfinance; limited free estimates |
| Polygon.io | Free tier / paid | API key | Prices, financials, news | Better for real-time; paid tier needed for full fundamentals |
| Seeking Alpha / Motley Fool | Web only | Scraping | Earnings transcripts | No clean API; scraping is fragile and ToS risk |
| OpenFIGI | Free | Optional key | Instrument identification, asset type | Useful for cross-referencing tickers |
| yfinance `t.insider_transactions` | Free | None | Insider buying and selling (Form 4 proxy) | Already available; not yet ingested |

---

## Update Log

| Date | Update |
|---|---|
| 2026-06-30 | Initial document created; sourced from framework.md analysis, Sprint 10 planning, and yfinance endpoint audit confirming all analyst estimate data is available without FMP |
