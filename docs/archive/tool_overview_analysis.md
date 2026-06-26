# Investment Research Tool
## Formal Module Specification

## 1. Purpose

The Investment Research Tool is a modular research and monitoring system designed to evaluate individual stocks, understand sector and industry conditions, and support investment decisions with structured analytics.

The tool should help answer three core questions:

1. **Is this company attractive on its own**
2. **How strong or weak is the sector or industry it operates in**
3. **How well does this investment fit into my portfolio or watchlist**

This tool is intended to support both exploratory research and repeatable monitoring. It should begin with notebook based workflows and evolve into reusable research modules, scorecards, and eventually LLM assisted summaries.

---

## 2. Design Principles

- Build in a modular way so each research layer can stand alone or feed downstream workflows
- Separate raw data ingestion from transformed research outputs
- Prioritize explainable metrics over black box scoring
- Support both snapshot research and change over time monitoring
- Keep the architecture flexible enough for Python and R implementations
- Design outputs so they can later feed a dashboard, report generator, or LLM summary layer

---

## 3. Core Modules

The tool will be organized into three primary modules:

1. **Company Research**
2. **Sector and Industry Research**
3. **Decision Support and Portfolio Fit**

---

# Module 1. Company Research

## 1.1 Objective

Evaluate an individual company from multiple angles including market performance, relative performance, business fundamentals, news and event context, and competitive positioning.

## 1.2 Key Questions

- How has the stock performed over time
- How has it performed relative to the market, sector, and peers
- Is the company fundamentally strong or weak
- Is the valuation rich, fair, or cheap
- What recent news, events, or sentiment may explain recent movement
- Who are the key competitors and where does the company sit in its landscape

## 1.3 Inputs

### Core identifiers
- ticker
- company name
- exchange
- sector
- industry
- sub industry

### Market data
- daily adjusted close
- open, high, low, close, volume
- historical market cap if available
- benchmark prices
- sector ETF prices
- peer stock prices

### Fundamental data
- revenue
- gross profit
- operating income
- net income
- EBITDA if available
- free cash flow
- earnings per share
- total debt
- cash
- shares outstanding
- margins
- return metrics
- valuation multiples
- analyst estimates if available

### News and event data
- article title
- article source
- publish date
- article summary or body snippet
- tagged themes
- sentiment score
- event type if classified

### Peer data
- peer companies
- peer prices
- peer fundamentals
- peer valuation multiples

## 1.4 Outputs

### Research outputs
- stock research summary table
- stock performance time series
- benchmark comparison table
- peer comparison table
- fundamentals summary table
- valuation summary
- news and sentiment digest
- competitor landscape summary
- company research scorecard

### Narrative outputs
- company overview
- strengths and weaknesses summary
- recent developments summary
- valuation context summary
- competitive position summary
- thesis and risk summary

## 1.5 KPI and Metric Dictionary

### A. Price performance metrics
- current price
- 1 day return
- 1 week return
- 1 month return
- 3 month return
- 6 month return
- year to date return
- 1 year return
- 3 year annualized return
- 5 year annualized return if available
- cumulative return over selected periods

### B. Risk and movement metrics
- annualized volatility
- rolling volatility
- max drawdown
- current drawdown from peak
- downside deviation
- beta versus benchmark
- average daily volume
- recent volume spike ratio
- gap up / gap down flags if relevant

### C. Trend and momentum metrics
- 50 day moving average
- 200 day moving average
- price versus 50 day moving average
- price versus 200 day moving average
- relative strength versus benchmark
- momentum score over trailing windows

### D. Relative performance metrics
- excess return versus S&P 500
- excess return versus sector ETF
- excess return versus peer median
- relative performance ranking within peer group

### E. Fundamental metrics
- revenue growth
- EPS growth
- EBITDA growth if available
- free cash flow growth
- gross margin
- operating margin
- net margin
- return on equity
- return on assets
- return on invested capital if available
- debt to equity
- net debt to EBITDA
- current ratio if available

### F. Valuation metrics
- price to earnings
- forward price to earnings if available
- EV to EBITDA
- price to sales
- price to free cash flow
- valuation premium or discount versus peers
- valuation premium or discount versus own historical average if available

### G. News and event metrics
- article count over trailing periods
- sentiment score average
- weighted sentiment score
- positive / neutral / negative article counts
- major event count
- earnings event flag
- regulatory event flag
- management change flag
- product / launch event flag

### H. Competitive landscape metrics
- peer market cap rank
- revenue rank within peers
- margin rank within peers
- valuation rank within peers
- return rank within peers

## 1.6 Visuals

### Core visuals
- stock price line chart over time
- indexed return comparison versus benchmark and peers
- drawdown chart
- rolling volatility chart
- valuation versus peers bar chart
- margins and growth comparison chart
- news sentiment over time
- peer positioning scatter plot such as growth versus margin or valuation versus growth

### Advanced visuals
- event annotated price chart
- peer quadrant map
- radar alternative using score bars instead of radar chart
- thesis scorecard table
- small multiple return charts for peers

## 1.7 Suggested Research Flow

1. Identify company metadata
2. Pull historical market data
3. Calculate return, volatility, and relative performance metrics
4. Pull core fundamentals
5. Pull peer group and calculate peer relative metrics
6. Pull recent news and classify sentiment and event types
7. Assemble scorecard and summary outputs
8. Generate narrative interpretation

## 1.8 Scoring Logic

Possible sub scores:

- **price strength score**
  based on trailing returns, moving average signals, and relative strength

- **risk score**
  based on volatility, drawdown, and beta

- **fundamental quality score**
  based on growth, margins, leverage, and returns on capital

- **valuation score**
  based on peer discount or premium and historical valuation context

- **sentiment and catalyst score**
  based on article sentiment, event frequency, and recent catalysts

- **competitive position score**
  based on market standing versus peers

Composite output:
- overall company research score
- confidence flag or data completeness flag

## 1.9 Data Sources

Potential sources:
- Yahoo Finance or other market price source
- Alpha Vantage, Financial Modeling Prep, Polygon, or another fundamentals source
- SEC filings for validation or fallback
- News APIs for article ingestion
- manually curated peer sets where needed

## 1.10 Phase Plan

### V1
- price history
- return metrics
- benchmark comparisons
- core fundamentals
- basic peer table

### V2
- valuation history
- richer peer set logic
- news ingestion and sentiment
- event tagging

### V3
- estimate revisions
- filing based text extraction
- deeper scoring and narrative generation

---

# Module 2. Sector and Industry Research

## 2.1 Objective

Evaluate the broader environment around a company by analyzing sector, industry, and sub industry performance, leadership, valuation, breadth, and major themes.

## 2.2 Key Questions

- Is the sector performing well or poorly
- Is performance broad based or concentrated in a few names
- Which companies are leading and lagging
- Are valuations stretched or attractive
- What structural trends or risks define the sector right now
- Is this company operating in a favorable or unfavorable market landscape

## 2.3 Inputs

### Classification data
- sector definitions
- industry definitions
- sub industry definitions
- mapping of stocks to classification structure

### Market data
- sector ETF prices
- industry ETF prices if available
- constituent stock prices
- market benchmark prices

### Fundamental and valuation data
- stock level fundamentals for sector constituents
- sector median valuation measures
- sector median growth and profitability measures

### News and thematic inputs
- sector related articles
- industry reports or summaries
- earnings call theme extraction if available
- regulatory or macro event flags

## 2.4 Outputs

- sector performance summary
- sector leadership table
- top and bottom performers table
- breadth and concentration view
- sector valuation summary
- sector fundamentals summary
- sector news and theme summary
- sector scorecard

## 2.5 KPI and Metric Dictionary

### A. Performance metrics
- sector return over trailing periods
- industry return over trailing periods
- sector return versus S&P 500
- sector return versus equal weighted market proxy if used
- industry return versus sector return

### B. Breadth metrics
- percentage of names with positive return over selected periods
- percentage of names above 50 day moving average
- percentage of names above 200 day moving average
- percentage outperforming benchmark
- equal weighted sector return
- cap weighted sector return

### C. Leadership and concentration metrics
- top contributors to sector return
- share of sector return from top 5 names
- market cap concentration
- performance dispersion across constituents

### D. Sector fundamentals
- median revenue growth
- median EPS growth
- median margin
- median return on equity
- median leverage
- median free cash flow margin

### E. Sector valuation
- median price to earnings
- median EV to EBITDA
- median price to sales
- current sector valuation versus own trailing history
- relative valuation versus overall market

### F. Theme and narrative metrics
- article count by topic
- theme frequency
- sentiment by topic
- regulatory / macro exposure flags

## 2.6 Visuals

### Core visuals
- sector indexed return line chart
- sector versus market relative performance chart
- bar chart of top and bottom performers
- breadth chart over time
- cap weighted versus equal weighted comparison
- valuation distribution chart
- growth versus margin scatter for sector constituents

### Advanced visuals
- market map of sector constituents
- industry subgroup comparison chart
- theme timeline
- contribution to sector return chart

## 2.7 Suggested Research Flow

1. Define sector, industry, and constituent universe
2. Pull historical price data for sector ETFs and constituents
3. Calculate relative performance and breadth
4. Pull stock level fundamentals and create sector aggregates
5. Pull sector level news and thematic information
6. Assemble leadership, valuation, and narrative outputs
7. Score sector strength and risk

## 2.8 Scoring Logic

Possible sub scores:

- **sector momentum score**
  based on trailing returns and relative performance

- **breadth score**
  based on percentage of names participating in the move

- **sector quality score**
  based on aggregate or median growth and profitability

- **sector valuation score**
  based on relative richness or cheapness

- **theme and sentiment score**
  based on positive or negative market narrative signals

Composite output:
- overall sector strength score

## 2.9 Data Sources

Potential sources:
- ETF price data
- constituent level market data providers
- company fundamentals sources
- news APIs
- manually maintained sector mappings if needed

## 2.10 Phase Plan

### V1
- sector ETF performance
- top sector stocks
- broad sector return and ranking tables

### V2
- breadth metrics
- sector valuation summaries
- industry subgroup analysis

### V3
- thematic extraction
- richer narrative synthesis
- dynamic sector score tracking

---

# Module 3. Decision Support and Portfolio Fit

## 3.1 Objective

Translate research into investment decision support by showing how a candidate stock fits into the existing portfolio, how it changes exposures, and how the research scores compare across watchlist names.

## 3.2 Key Questions

- Does this stock improve or worsen concentration risk
- Does it diversify the portfolio
- Is it highly correlated with what I already own
- What role would it play in the portfolio
- How does it compare with other candidates
- Is it a buy now, monitor, or avoid candidate based on current evidence

## 3.3 Inputs

- current holdings
- portfolio weights
- candidate stock metrics from Module 1
- sector metrics from Module 2
- historical returns for candidate and holdings
- security metadata
- optional target allocation assumptions

## 3.4 Outputs

- portfolio fit report
- correlation summary
- concentration impact view
- sector exposure impact
- candidate ranking table
- watchlist summary
- decision support scorecard

## 3.5 KPI and Metric Dictionary

### A. Portfolio fit metrics
- candidate weight assumption
- change in sector exposure
- change in top holdings concentration
- change in Herfindahl style concentration measure if desired
- overlap with current holdings by sector and theme
- diversification contribution

### B. Relationship metrics
- correlation with portfolio
- correlation with major holdings
- beta to portfolio proxy
- overlap with current factor or sector exposures

### C. Decision metrics
- company research score
- sector strength score
- valuation score
- fit score
- watchlist priority rank

### D. Monitoring metrics
- score change over time
- valuation threshold flags
- price movement alerts
- earnings date / event flags
- sentiment deterioration or improvement

## 3.6 Visuals

- portfolio before and after allocation bar chart
- sector exposure impact chart
- candidate versus holdings correlation heatmap
- watchlist ranking table
- score trend line over time
- candidate comparison scorecard bars

## 3.7 Suggested Research Flow

1. Pull candidate outputs from Modules 1 and 2
2. Pull current portfolio holdings and exposures
3. Calculate correlations and overlap
4. Simulate proposed weight or purchase size
5. Compare candidate versus other watchlist ideas
6. Create ranked recommendation or watchlist summary

## 3.8 Scoring Logic

Possible sub scores:

- **portfolio fit score**
  based on diversification, overlap, and exposure impact

- **timing score**
  based on valuation, momentum, and recent catalysts

- **watchlist priority score**
  combined score reflecting attractiveness and fit

Composite outputs:
- buy candidate
- monitor candidate
- low priority candidate

These should remain explainable labels rather than automated investment advice.

## 3.9 Data Sources

- existing portfolio holdings data
- outputs from company research
- outputs from sector research
- manual watchlist and notes table

## 3.10 Phase Plan

### V1
- portfolio fit summary
- basic watchlist ranking

### V2
- correlation and concentration modeling
- score tracking over time

### V3
- alerts
- what if allocation scenarios
- LLM generated investment summaries

---

## 4. Cross Module Supporting Components

# 4.1 Security Master

A shared reference layer for:
- ticker
- company name
- sector
- industry
- sub industry
- exchange
- currency
- market cap bucket
- manually curated tags
- peer group overrides

# 4.2 Peer Set Engine

Rules for generating peer groups:
- same industry or sub industry
- similar market cap band
- manual overrides
- curated competitor lists

# 4.3 News and Event Classification Layer

Shared logic for:
- ingesting articles
- deduplicating articles
- assigning sentiment
- classifying event type
- tagging themes
- storing article scores over time

# 4.4 Scoring Framework

Central score definitions:
- company score
- sector score
- valuation score
- fit score
- watchlist score

All scores should store:
- raw metric inputs
- normalized values
- weightings
- final score
- score date

# 4.5 LLM Summary Layer

Future layer that reads structured outputs and generates:
- company memo
- sector memo
- investment thesis summary
- weekly watchlist update
- event recap

---

## 5. Suggested Data Architecture

## 5.1 Logical Layers

### Raw layer
Stores raw API pulls and downloaded datasets:
- raw_prices
- raw_fundamentals
- raw_news
- raw_sector_mappings
- raw_benchmarks

### Staging layer
Standardizes identifiers and schemas:
- stg_prices_daily
- stg_company_fundamentals
- stg_news_articles
- stg_sector_classification
- stg_benchmark_prices

### Research layer
Creates reusable analytics tables:
- research_stock_returns
- research_stock_risk_metrics
- research_stock_fundamental_metrics
- research_stock_valuation_metrics
- research_news_sentiment
- research_peer_comparison
- research_sector_performance
- research_sector_breadth
- research_sector_valuation
- research_portfolio_fit

### Presentation layer
Used by notebooks, dashboards, and LLM prompts:
- mart_company_scorecard
- mart_sector_scorecard
- mart_watchlist_rankings
- mart_stock_research_summary
- mart_sector_research_summary
- mart_investment_decision_summary

## 5.2 Key Entities

- securities
- prices
- benchmarks
- fundamentals
- valuation metrics
- peer groups
- sector mappings
- news articles
- sentiment scores
- event tags
- watchlist entries
- research scores

---

## 6. Notebook and Reporting Structure

## 6.1 Company Research Notebook
Sections:
1. Company overview
2. Price performance
3. Relative performance
4. Fundamentals
5. Valuation
6. News and events
7. Peer comparison
8. Summary and scorecard

## 6.2 Sector Research Notebook
Sections:
1. Sector overview
2. Performance and ranking
3. Breadth
4. Leadership and laggards
5. Valuation and fundamentals
6. Key themes and news
7. Sector scorecard

## 6.3 Decision Support Notebook
Sections:
1. Candidate summary
2. Portfolio overlap and fit
3. Correlation and diversification
4. Candidate ranking versus watchlist
5. Decision summary

---

## 7. Minimum Viable Product

The first version should focus on the highest value core.

### MVP scope
- single stock research notebook
- price history and return metrics
- benchmark comparison
- core fundamentals
- simple peer comparison
- simple sector summary
- basic news ingestion
- initial scorecard

### MVP outputs
- one reusable stock research report
- one sector context report
- one watchlist ranking table

---

## 8. Future Enhancements

- earnings transcript analysis
- SEC filing parsing
- estimate revision tracking
- alternative data or social sentiment
- valuation regime analysis
- macro sensitivity analysis
- alerting and automated refresh
- interactive dashboard
- LLM generated research memos

---

## 9. Risks and Design Notes

- News sentiment can be noisy and should not dominate the scoring
- Peer quality is highly dependent on peer selection rules
- Fundamentals from free APIs may be incomplete or inconsistent
- Sector classification source should be standardized early
- Composite scores must remain interpretable
- Non US or non equity securities may require separate logic later

---

## 10. Recommended Build Order

### Phase 1
Build a reusable **Company Research** workflow:
- security master
- price ingestion
- benchmark comparison
- fundamentals pull
- simple peer table
- company scorecard

### Phase 2
Build **Sector and Industry Research**:
- sector mapping
- sector ETF and constituents
- sector performance tables
- breadth and valuation views
- sector scorecard

### Phase 3
Build **Decision Support**:
- watchlist table
- candidate ranking
- portfolio fit
- monitoring logic

### Phase 4
Build **Narrative and Intelligence Layer**:
- structured prompts
- LLM summaries
- automated research notes

---

## 11. Success Criteria

The tool is successful if it can:

- produce a repeatable stock research report for any selected ticker
- compare the stock to appropriate benchmarks and peers
- summarize sector context clearly
- score a stock across performance, quality, valuation, and sentiment
- support watchlist comparison and decision support
- generate structured outputs that can later feed a dashboard or LLM layer