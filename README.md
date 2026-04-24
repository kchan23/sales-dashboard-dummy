# CPP '26 MSBA Culminating Project - Sales Dashboard w/ Dummy Data

A data analytics dashboard for Dough Zone that visualizes sales, menu performance, inventory, and customer reviews.

Public-safe Streamlit demo for restaurant operations analytics.

This repository is configured for presentation mode first. The default app path uses synthetic data from `demo_data/` and does not require BigQuery, GCS, Toast, or Instagram credentials.

Run locally:

```bash
streamlit run app.py
```

If you need the live integrations, explicitly set `DEMO_MODE=false` and provide the relevant credentials.

## Status

Currently converting from React prototype to Streamlit application for easier deployment and data processing.

## Integration Layout

External data-source code lives under `integrations/`:

- `integrations/toast_api/` for Toast ingestion into BigQuery
- `integrations/instagram_api/` for Instagram Graph snapshot ingestion into BigQuery

Instagram support is backend-only in this repo. It exists for warehouse completeness and tests, and is not rendered in the Streamlit presentation UI.

## Project Structure

```
sales-dashboard-dummy/
├── app.py
├── demo_data/
├── database/
├── integrations/
│   ├── toast_api/
│   └── instagram_api/
├── scripts/
├── documentation/
└── tests/
```

## Data Structure

Data is organized by location ID and date:
- **Location**: Restaurant location code (e.g., `90984`)
- **Date**: Folder in `YYYYMMDD` format (e.g., `20250116` for Jan 16, 2025)

Expected files in each date folder:
- Parquet files: `OrderDetails`, `PaymentDetails`, `TimeEntries`, `ItemSelectionDetails`, `CheckDetails`
- JSON: Various formats (parsed per file)

## Features

- **Sales Analytics**: Revenue and order trends over time
- **Menu Performance**: Top items by revenue and order count
- **Customer Analytics**: PII-masked customer metrics for demo-safe presentation
- **Presentation Mode**: Demo-first runtime with no required external credentials
- **Optional Live Integrations**: Toast and Instagram ingestion remain available for non-demo use

## Development

See [docs/CLAUDE.md](./docs/CLAUDE.md) for detailed development guidance and architecture notes.

### Notes
- The Streamlit UI remains presentation-focused.
- Live sync paths are optional and stay out of the default demo experience.
- Instagram snapshot tables/views are included for backend completeness, not for UI display.

## Git Repository

This project is a Git repository ready for GitHub deployment. See [docs/CLAUDE.md](./docs/CLAUDE.md) for repository guidelines.
