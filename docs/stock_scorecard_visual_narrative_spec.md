# Stock Scorecard Visual and Narrative Specification

## Purpose

This document defines the visual structure and narrative flow for a stock scorecard report within the Investment Research Tool.

The goal is to make the stock scorecard feel like a research memo rather than a disconnected dashboard. The report should explain:

1. how the stock has behaved
2. why it has behaved that way
3. whether the business is strong
4. whether the balance sheet is sound
5. whether the valuation is justified
6. what the total score implies

This specification is designed for notebook based implementation first, with later reuse in dashboards, static reports, or LLM generated summaries.

---

## Design Principles

- move from price behavior to business quality to valuation and conclusion
- prioritize clear, high signal visuals over chart quantity
- make the score transparent rather than black box
- use peer comparisons where they add context
- keep each section tied to a specific research question
- support a narrative that can be summarized by an LLM later

---

## Recommended Report Flow

The stock scorecard report should follow this sequence:

1. Research Summary
2. Market Performance
3. Trend and Risk Profile
4. Growth and Profitability
5. Balance Sheet Strength
6. Valuation
7. Peer Positioning
8. Score Decomposition
9. Final Research Summary

This sequence should move from:

**what happened -> why it happened -> what the business looks like -> what the risks are -> whether the price makes sense**

## Current Implementation Status

The repo now has a working first-pass company research notebook and shared report helpers, but it does not yet satisfy the full visual memo structure defined here.

What currently exists:

- company overview table
- score summary table
- trailing return table
- benchmark comparison table
- category score chart
- excess return chart
- metric score table and chart

What is still missing from this spec:

- indexed price performance chart
- drawdown chart
- price plus moving-average chart
- dedicated risk metric panel
- growth and profitability trend visuals
- balance-sheet narrative table with interpretation flags
- valuation comparison visual
- peer positioning visuals
- final memo-style narrative summary

This means the spec should be treated as:

- `V1 narrative target` for the single-ticker workflow
- not a claim that all sections are already backed by current data

---

# Section 1. Research Summary

## Narrative Goal

Orient the reader immediately and summarize what kind of stock this is, what drives the score, and what is holding it back.

## Key Questions

- What is the total score
- Which score pillars are strongest and weakest
- What is the high level interpretation of the company

## Required Inputs

- company name
- ticker
- sector
- industry
- report date
- total score
- category scores
- interpretation label
- short strengths and weaknesses summary

### Current data readiness

- available now:
  company metadata, total score, category scores, current scorecard summary
- needs new derived output:
  interpretation label, standardized strengths, standardized weaknesses

## Transformation Steps

1. Pull the latest stock level score outputs
2. Extract the total score and category level scores
3. Map the score profile to a short interpretation label
4. Assemble a compact summary table or panel

## Output Visual

### Visual 1: Score Summary Strip

A horizontal bar set showing:
- total score
- market performance score
- growth and profitability score
- balance sheet strength score
- valuation score

## Recommended Chart Type

- horizontal bar chart
- compact scorecard strip
- small summary table

## Narrative Notes

This section should include one short paragraph covering:
- what kind of company this is
- what is driving the score
- what is constraining the score

---

# Section 2. Market Performance

## Narrative Goal

Show whether the stock has actually been a strong investment and whether it has outperformed the broader market and sector.

## Key Questions

- Has the stock outperformed over time
- Is recent performance consistent with longer term performance
- Has the stock beaten the broader market and sector benchmark

## Required Inputs

- historical stock prices
- benchmark prices such as S&P 500
- sector ETF prices
- trailing return metrics
- selected lookback periods

### Current data readiness

- available now:
  normalized company price history, benchmark price history, trailing return outputs
- partial:
  sector ETF or style benchmark assignment is still temporary and should be formalized
- recommended update:
  move benchmark comparison inputs toward a shared benchmark mapping layer rather than notebook assumptions

## Transformation Steps

1. Pull adjusted close prices for the stock, benchmark, and sector ETF
2. Rebase each series to a common starting point such as 100
3. Calculate trailing returns across key windows
4. Align all dates and standardize the display period

## Output Visuals

### Visual 2: Indexed Price Performance Chart

Plot:
- stock
- S&P 500
- sector ETF

All series should be indexed to 100 at the start of the display window.

### Visual 3: Trailing Return Comparison

Show bars for:
- 1 month
- 3 month
- 6 month
- 12 month
- 3 year annualized if available

Compare:
- stock
- market benchmark
- sector benchmark

## Recommended Chart Type

- line chart for indexed returns
- grouped bar chart for trailing returns

## Narrative Notes

Discuss:
- long term versus recent performance
- whether outperformance is broad market driven or sector driven
- whether the stock is a leader or laggard in context

---

# Section 3. Trend and Risk Profile

## Narrative Goal

Show the quality of the stock’s price action and whether returns came with manageable risk.

## Key Questions

- Is the stock in a healthy uptrend or unstable pattern
- How severe have drawdowns been
- Is volatility reasonable for the return profile

## Required Inputs

- stock price history
- moving averages
- drawdown calculations
- volatility metrics
- beta
- downside risk metrics

### Current data readiness

- available now:
  moving averages, volatility, max drawdown, current drawdown
- missing now:
  beta, downside volatility, benchmark drawdown comparison output
- recommended update:
  add reusable derived risk series and risk summary outputs before adding more notebook narrative

## Transformation Steps

1. Calculate 50 day and 200 day moving averages
2. Compute running peak and drawdown series
3. Calculate annualized volatility and current drawdown
4. Pull or estimate beta versus benchmark

## Output Visuals

### Visual 4: Drawdown Chart

Plot stock drawdown over time, optionally with benchmark drawdown for comparison.

### Visual 5: Price and Moving Averages

Plot stock price with:
- 50 day moving average
- 200 day moving average

### Visual 6: Risk Metric Panel

Summarize:
- annualized volatility
- beta
- max drawdown
- current drawdown
- downside volatility

## Recommended Chart Type

- line chart for drawdown
- line chart for price plus moving averages
- compact summary table or bar panel for risk metrics

## Narrative Notes

Discuss:
- whether the trend is intact
- whether volatility is acceptable
- whether the stock’s risk profile supports or weakens the investment case

---

# Section 4. Growth and Profitability

## Narrative Goal

Show whether the company is growing, profitable, and improving as a business.

## Key Questions

- Is the company growing revenue and earnings
- Are margins healthy and improving
- Is the business converting growth into profitability and cash flow

## Required Inputs

- revenue history
- EPS history
- operating margin history
- free cash flow margin history
- ROIC or ROE history
- peer medians if available

### Current data readiness

- available now:
  latest profitability snapshot values
- missing now:
  revenue history, EPS history, margin history, peer medians
- required future data work:
  historical fundamentals ingestion and staging conventions

## Transformation Steps

1. Pull annual or quarterly financial history
2. Calculate growth rates and margin trends
3. Standardize reporting periods
4. Add peer medians or benchmark levels if available

## Output Visuals

### Visual 7: Revenue and EPS Growth Trend

Show growth trends over time for:
- revenue
- EPS

### Visual 8: Margin Trend Chart

Show:
- operating margin
- free cash flow margin
- gross margin if relevant

### Visual 9: Return on Capital Summary

Show recent trend or recent period values for:
- ROIC or ROE
- peer median if available

## Recommended Chart Type

- line chart for growth trends
- line chart for margin trends
- bar chart or line chart for return on capital

## Narrative Notes

Discuss:
- whether growth is accelerating or decelerating
- whether margins are expanding or compressing
- whether the business quality is improving, stable, or weakening

---

# Section 5. Balance Sheet Strength

## Narrative Goal

Evaluate the company’s financial resilience and flexibility.

## Key Questions

- Is leverage manageable
- Can the company service its debt
- Does it have enough liquidity to handle stress

## Required Inputs

- total debt
- cash
- EBITDA
- interest expense
- current assets
- current liabilities
- free cash flow
- peer medians or selected peer metrics

### Current data readiness

- available now:
  total debt, cash, EBITDA, free cash flow, current ratio, debt to equity
- missing now:
  interest expense, current assets, current liabilities, peer medians
- recommended near-term update:
  build a point-in-time balance-sheet table now, then expand once the missing raw fields are modeled

## Transformation Steps

1. Calculate leverage metrics
2. Calculate coverage and liquidity ratios
3. Compare the company to peer medians or selected peers
4. Map values to flags such as strong, neutral, or weak

## Output Visuals

### Visual 10: Balance Sheet Scorecard Table

Include:
- net debt to EBITDA
- debt to equity
- interest coverage
- current ratio
- free cash flow to debt

For each metric show:
- company value
- peer median
- score contribution
- interpretation flag

### Visual 11: Leverage Versus Peers

Compare the company with selected peers on one or two core leverage metrics.

## Recommended Chart Type

- scorecard table
- dot plot
- grouped bar chart

## Narrative Notes

Discuss:
- whether leverage is conservative or stretched
- whether financial structure is a support or risk
- whether balance sheet quality is a differentiator versus peers

---

# Section 6. Valuation

## Narrative Goal

Show whether the stock appears expensive or attractive relative to peers, growth, and its own quality.

## Key Questions

- Is the stock trading at a premium or discount
- Is the valuation justified by growth and quality
- Is valuation the main constraint on the score

## Required Inputs

- forward P/E
- EV/EBITDA
- price to sales or EV/sales if relevant
- free cash flow yield
- peer valuation metrics
- growth metrics
- historical valuation data if available

### Current data readiness

- available now:
  forward P/E, EV/EBITDA, price to sales, dividend yield, derived free cash flow yield
- missing now:
  peer valuation context, historical valuation series, growth context for valuation-versus-growth scatter
- recommended near-term update:
  start with a current-state valuation table or simple comparison visual before adding peer or history layers

## Transformation Steps

1. Pull current valuation multiples
2. Compare with peer group and sector medians
3. Link valuation multiples to growth and profitability context
4. Calculate historical valuation bands if available

## Output Visuals

### Visual 12: Valuation Versus Peers

Compare:
- forward P/E
- EV/EBITDA
- free cash flow yield

Show company versus peer set.

### Visual 13: Valuation Versus Growth Scatter

Plot:
- x axis = growth metric such as revenue growth or EPS growth
- y axis = valuation multiple such as forward P/E or EV/EBITDA
- highlight the target company

### Optional Visual 14: Historical Valuation Band

Show the company’s valuation multiple over time against its own historical range.

## Recommended Chart Type

- dot plot or grouped bar chart
- scatter plot
- line chart for historical valuation

## Narrative Notes

Discuss:
- whether the stock is richly valued, fair, or attractive
- whether its premium or discount is earned
- whether valuation risk is likely to limit future returns

---

# Section 7. Peer Positioning

## Narrative Goal

Place the company in context versus direct competitors and comparable firms.

## Key Questions

- Where does this company rank within the peer set
- Is it a leader, middle tier name, or laggard
- Which peer is the most relevant comparison

## Required Inputs

- peer list
- peer market cap
- peer growth metrics
- peer profitability metrics
- peer leverage metrics
- peer valuation metrics
- peer return metrics
- peer total scores if available

### Current data readiness

- not available yet:
  peer definitions, peer prices, peer fundamentals, peer scores
- required future data work:
  peer-set modeling and peer-level marts

## Transformation Steps

1. Construct a peer set using industry and market cap logic
2. Pull key market and fundamental metrics for each peer
3. Rank the target company across key dimensions
4. Assemble a comparison table and highlight the target company

## Output Visuals

### Visual 15: Peer Comparison Table

Suggested columns:
- market cap
- revenue growth
- operating margin
- net debt to EBITDA
- forward P/E
- 12 month return
- total score

### Visual 16: Peer Positioning Scatter

Good default options:
- growth versus margin
- valuation versus growth
- return versus volatility

Highlight the company and a few closest peers.

## Recommended Chart Type

- formatted comparison table
- scatter plot with labels

## Narrative Notes

Discuss:
- where the company sits in the competitive landscape
- whether it is differentiated by quality, valuation, or performance
- which peers best frame the investment case

---

# Section 8. Score Decomposition

## Narrative Goal

Explain clearly why the stock received its total score.

## Key Questions

- Which categories contribute most to the score
- Which metrics are strongest and weakest
- Are weak areas temporary or structural

## Required Inputs

- total score
- category scores
- metric level scores
- metric weights
- peer or benchmark percentile ranks
- score interpretation rules

### Current data readiness

- available now:
  total score, category scores, metric scores, metric weights, confidence score
- missing now:
  peer percentile columns and fully standardized interpretation rules
- recommended near-term update:
  improve score decomposition visuals using current score outputs before layering in peer percentiles

## Transformation Steps

1. Pull metric level scoring outputs
2. Aggregate by category
3. Calculate category contribution to total score
4. Highlight strongest and weakest metric contributions

## Output Visuals

### Visual 17: Score Contribution Chart

Show contributions from:
- market performance
- growth and profitability
- balance sheet strength
- valuation

### Visual 18: Metric Score Heatmap

Rows:
- metrics

Columns:
- score bucket
- raw value
- peer percentile
- metric score

Or alternatively compare:
- target company
- peer median

## Recommended Chart Type

- waterfall chart
- stacked bar chart
- heatmap table

## Narrative Notes

Discuss:
- strongest scoring areas
- weakest scoring areas
- whether the score reflects a well balanced profile or one driven by only one dimension

---

# Section 9. Final Research Summary

## Narrative Goal

Close the report with a concise and actionable interpretation.

## Key Questions

- What is the core thesis
- What are the main risks
- What should be watched next

## Required Inputs

- total score
- score interpretation label
- major strengths
- major risks
- recent catalysts
- valuation interpretation
- watch items

### Current data readiness

- available now:
  total score, category strengths and weaknesses can be derived from current outputs
- missing now:
  recent catalysts, formal watch items, narrative labeling rules
- recommended near-term update:
  implement a simple structured closing summary driven by score outputs before adding news or event data

## Transformation Steps

1. Pull the top strengths and risks from the scorecard
2. Incorporate notable recent events or catalysts
3. Create a concise final summary
4. Standardize into a reusable closing format

## Output Visual

No additional chart is required. A structured summary card is sufficient.

## Recommended Summary Structure

### What is working
- top strengths

### What is not working
- top risks

### Bottom line
- one to three sentence conclusion

### What to watch
- earnings
- valuation
- margin trend
- debt changes
- sector shifts

## Narrative Notes

The final summary should read like the closing paragraph of a research memo. It should synthesize the visuals rather than repeat them.

---

## Recommended V1 Visual Set

The full framework includes many possible visuals, but the first version should stay focused.

### Recommended V1 visuals

1. score summary strip
2. indexed price performance chart
3. trailing return comparison
4. drawdown chart
5. revenue and EPS growth trend
6. margin trend chart
7. balance sheet scorecard table
8. valuation versus peers chart

This set provides a complete and credible initial report while keeping implementation manageable.

### Recommended implementation sequencing

The first implementation pass should be split into:

1. visuals supported by current data
2. visuals that require new historical, benchmark, or peer data

Recommended `build now` set:

1. score summary strip
2. indexed price performance chart
3. trailing return comparison
4. drawdown chart
5. price plus moving averages
6. risk metric panel
7. balance sheet scorecard table
8. valuation summary table
9. score decomposition chart

Recommended `build later` set:

1. revenue and EPS growth trend
2. margin trend chart
3. valuation versus peers
4. peer comparison table
5. peer positioning scatter

---

## Notebook or Report Structure

The report or notebook should follow this structure:

1. Research Summary
2. Stock Performance
3. Trend and Risk
4. Growth and Profitability
5. Balance Sheet
6. Valuation
7. Peer Positioning
8. Score Decomposition
9. Bottom Line

---

## Suggested Data Dependencies

### Company level
- price history
- benchmark prices
- sector ETF prices
- trailing returns
- drawdown metrics
- moving averages
- volatility metrics

### Fundamental level
- revenue
- EPS
- margins
- free cash flow
- ROIC or ROE
- leverage metrics
- liquidity metrics
- valuation multiples

### Peer level
- peer definitions
- peer prices
- peer fundamentals
- peer valuations
- peer return metrics

### Score outputs
- category scores
- metric scores
- total score
- confidence score
- interpretation label

## Data Model and Staging Notes

The narrative workflow implies a few concrete shared-data follow-ons that should be tracked explicitly.

### Needed soon

- a benchmark mapping output for `primary_benchmark`, `sector_benchmark`, and `style_benchmark`
- reusable derived price series for indexed performance, moving averages, and drawdowns
- standardized score interpretation outputs such as label, strengths, and weaknesses

### Needed later

- historical fundamentals snapshots for growth and margin visuals
- peer-set outputs for comparison sections
- historical valuation series if valuation-band visuals are desired

These are shared data or analytics needs, not notebook-only concerns.

## Sprint Alignment Note

This spec remains aligned with Sprint 3 as long as we treat:

- the polished single-ticker narrative notebook and the visuals supported by current data as Sprint 3 work
- benchmark mapping formalization, historical fundamentals expansion, and peer modeling as the next enabling layer rather than required completion criteria for the current sprint

---

## Design Notes

- Keep the visual order aligned to the research story rather than the data pipeline
- Use peer comparisons only where they improve understanding
- Avoid overly decorative charts
- Favor bars, lines, scatter plots, and compact score tables
- Make score logic visible so the report remains interpretable
- Reserve more advanced visuals such as valuation bands and peer heatmaps for later phases

---

## Recommended Next Step

The next build artifact should be a visual dictionary for each chart, including:

- chart name
- business goal
- input tables
- transformation logic
- x axis
- y axis
- color or grouping logic
- annotations
- implementation notes in Python and R

That can then be used to build the notebook and reporting layer systematically.
