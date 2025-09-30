# RFM Customer Segmentation Model

## Overview

This project implements an RFM (Recency, Frequency, Monetary) analysis model to segment customers based on their behavioral patterns and predict future engagement.

## What is RFM?

RFM analysis scores customers on three dimensions:
- **Recency**: How recently did the customer interact?
- **Frequency**: How often do they engage?
- **Monetary**: What's the total value of their interactions?

## Key Features

- Automated customer segmentation
- Multi-source data integration
- Configurable scoring weights
- AutoML-powered predictions
- Treasure Data native integration

## Project Structure

```
rfm_prod/
├── config/
│   └── input_params.yml    # Main configuration file
├── queries/                # SQL queries for data processing
├── notebooks/              # Analysis notebooks
└── QUICKSTART.md          # Configuration guide
```

## Requirements

- Treasure Data account
- Access to customer behavior tables
- AutoML enabled (or custom model option)

## Getting Started

See [QUICKSTART.md](QUICKSTART.md) for detailed configuration instructions.

## Model Overview

### How It Works

1. **Data Integration**: Collects customer interactions from multiple sources (web visits, purchases, leads, etc.)
2. **RFM Calculation**:
   - Recency: Days since last interaction
   - Frequency: Total number of interactions
   - Monetary: Sum of all interaction values (weighted)
3. **Scoring**: Each dimension scored 1-5 (5 being best)
4. **Segmentation**: Creates customer segments based on RFM combinations

### Model Types

- **Custom**: SQL-based segmentation using predefined RFM rules and thresholds
- **AutoML**: Machine learning clustering approach that automatically discovers optimal segments

### RFM Segments

| Segment | RFM Score | Description |
|---------|-----------|-------------|
| Champions | 555, 554, 544, 545, 454, 455, 445 | Best customers - recent, frequent, high value |
| Loyal Customers | 543, 444, 435, 355, 354, 345, 344, 335 | Good frequency and monetary value |
| Potential Loyalists | 553, 551, 552, 541, 542, 533, 532, 531, 452, 451 | Recent customers with potential |
| New Customers | 512, 511, 422, 421, 412, 411, 311 | Recently acquired |
| Promising | 525, 524, 523, 522, 521, 515, 514, 513, 425, 424, 413, 414, 415, 315, 314, 313 | Recent but low frequency |
| Need Attention | 535, 534, 443, 434, 343, 334, 325, 324 | Above average recency/frequency/monetary |
| About to Sleep | 331, 321, 312, 221, 213, 231, 241, 251 | Below average recency/frequency |
| At Risk | 255, 254, 245, 244, 253, 252, 243, 242, 235, 234, 225, 224, 153, 152, 145, 143, 142, 135, 134, 133, 125, 124 | Haven't purchased recently |
| Can't Lose Them | 155, 154, 144, 214, 215, 115, 114, 113 | High value but losing them |
| Hibernating | 332, 322, 231, 241, 251, 233, 232, 223, 222, 132, 123, 122, 212, 211 | Low recency and frequency |
| Lost | 111, 112, 121, 131, 141, 151 | Lowest engagement across all dimensions |

## Output

The model generates:
- Customer segments with RFM scores
- Propensity predictions
- Statistical summaries
- Unified activity data

## Support

For questions or issues, contact your Treasure Data representative.