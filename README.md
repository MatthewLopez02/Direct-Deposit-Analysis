# Direct Deposit Analysis Report

Interactive dashboard analyzing ACH/Direct Deposit transactions from MyBambu users.

**Live Report:** https://matthewlopez02.github.io/Direct-Deposit-Analysis/

---

## Features

- **7 Preset Date Ranges:**
  - Last 30 Days
  - Last 60 Days
  - Last 90 Days
  - August 2025
  - September 2025
  - October 2025
  - November 2025

- **Key Metrics:**
  - Unique users with direct deposits
  - Total transactions
  - Total volume
  - Average deposit amount

- **Interactive Charts:**
  - Transaction amount distribution (buckets)
  - Top 10 ACH institutions by frequency
  - Top 10 ACH institutions by volume

---

## Automated Daily Updates

This report automatically refreshes data **daily at 6 AM UTC** (1 AM EST / 2 AM EDT) via GitHub Actions.

### How It Works

1. **GitHub Actions Workflow** (`.github/workflows/daily-update.yml`) runs on schedule
2. **Python Script** (`update_data.py`) queries Snowflake for all 7 date ranges
3. **HTML File** (`index.html`) is automatically updated with fresh data
4. **GitHub Pages** serves the updated report instantly

### Setup Instructions

To enable automated updates, you need to configure GitHub repository secrets:

1. Go to **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret** and add the following:

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `SNOWFLAKE_ACCOUNT` | Your Snowflake account identifier | `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | Snowflake username | `your_username` |
| `SNOWFLAKE_PASSWORD` | Snowflake password | `your_password` |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name | `COMPUTE_WH` |

### Manual Trigger

You can manually trigger a data update:

1. Go to **Actions** tab
2. Select **Daily Direct Deposit Data Update** workflow
3. Click **Run workflow**

---

## Data Source

- **Database:** `MYBAMBU_PROD`
- **Schema:** `BAMBU_MART_GALILEO`
- **Table:** `MART_SRDF_POSTED_TRANSACTIONS`
- **Filter:** `TRANSACTION_CODE = 'PMOF'` (ACH direct deposits)

---

## Technical Stack

- **Frontend:** HTML, CSS, Chart.js
- **Backend:** Python 3.11, Snowflake Connector
- **Automation:** GitHub Actions
- **Hosting:** GitHub Pages

---

## Local Development

### Prerequisites

```bash
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file or set environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
```

### Run Update Script

```bash
python update_data.py
```

This will query Snowflake and update `index.html` with the latest data.

---

## File Structure

```
.
├── .github/
│   └── workflows/
│       └── daily-update.yml    # GitHub Actions workflow
├── index.html                   # Main report (auto-updated)
├── update_data.py              # Data refresh script
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

---

## Created By

**Paid Ads Team**

---

## Notes

- Report is marked with `noindex, nofollow` meta tags
- Data is refreshed daily but historical months (Aug/Sep/Oct/Nov 2025) remain static
- Dynamic ranges (Last 30/60/90 Days) update based on current date
